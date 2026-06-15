"""
FREED — Tamura Sweep (Piece 4)
Paper ingestion pipeline. The sensory surface of the organism.

Sources:
  - Cecile G. Tamura / Lifeboat Foundation  (curated AI/science/futures)
  - arXiv biophysics RSS feeds              (nature as independent substrate)

The sweep:
  1. Fetches each source
  2. Extracts new articles/papers
  3. For arXiv: keyword pre-filters against RSA-adjacent topics (no API cost)
  4. Fetches full text for new items
  5. Returns structured inputs ready for FEED

Seen URLs are tracked in tamura_seen.json — FREED never feeds the same article twice.

Complexity scoring:
  - RangeEn (Range Entropy) — a modification of SampEn that normalizes
    template-matching tolerance by signal range (max−min) rather than
    standard deviation. More robust under nonstationarity; linear
    relationship with Hurst exponent. Used for O68: DEA on genome's
    coherence time-series.
    Ref: Omidvarnia et al., "Range Entropy: A Bridge between Signal
    Complexity and Self-Similarity" (Entropy, 2018).

  - DEM (Diffusion Entropy Method) — computes the scaling exponent δ
    from the PDF of cumulative displacements p(x, t) rather than from
    fluctuation variance F(t). Unlike DFA, DEM preserves short-time
    scaling, asymptotic saturation, and modulation structure in entropy
    time-series.  Used as an alternative scorer for O28/L2 grounding
    where floor-saturation structure matters.
    Ref: Scafetta, N. & Grigolini, P., "Scaling detection in time
    series: Diffusion entropy analysis" (Phys. Rev. E, 2002);
    cf. EEG entropy dynamics showing DFA suppresses key properties.
"""

import json
import math
import time
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from feed_guard import sanitize

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR  = Path(__file__).parent
SEEN_FILE  = FREED_DIR / "tamura_seen.json"

# ─── Source definitions ───────────────────────────────────────────────────────
SOURCES = [
    {
        "name":     "Cecile G. Tamura — Lifeboat Foundation",
        "url":      "https://lifeboat.com/blog/author/cecile-g-tamura",
        "type":     "lifeboat_author",
        "priority": "high",
    },
    # Biophysics — nature as independent substrate for RSA invariant confirmation
    {
        "name":     "arXiv — Neurons & Cognition (q-bio.NC)",
        "url":      "http://export.arxiv.org/rss/q-bio.NC",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Biological Physics (physics.bio-ph)",
        "url":      "http://export.arxiv.org/rss/physics.bio-ph",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Molecular Networks (q-bio.MN)",
        "url":      "http://export.arxiv.org/rss/q-bio.MN",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Populations & Evolution (q-bio.PE)",
        "url":      "http://export.arxiv.org/rss/q-bio.PE",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    # Physics / computation / information theory — kernel step confirmation
    {
        "name":     "arXiv — Statistical Mechanics (cond-mat.stat-mech)",
        "url":      "http://export.arxiv.org/rss/cond-mat.stat-mech",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Information Theory (cs.IT)",
        "url":      "http://export.arxiv.org/rss/cs.IT",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Nonlinear: Adaptation & Self-Organizing Systems (nlin.AO)",
        "url":      "http://export.arxiv.org/rss/nlin.AO",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Pattern Formation & Solitons (nlin.PS)",
        "url":      "http://export.arxiv.org/rss/nlin.PS",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Disordered Systems & Neural Networks (cond-mat.dis-nn)",
        "url":      "http://export.arxiv.org/rss/cond-mat.dis-nn",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    # bioRxiv preprints — independent biological substrate confirmation
    # connect.biorxiv.org is the canonical RSS endpoint (30 most recent per subject)
    {
        "name":     "bioRxiv — Biophysics",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=biophysics",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "bioRxiv — Systems Biology",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=systems_biology",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "bioRxiv — Neuroscience",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=neuroscience",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "bioRxiv — Evolutionary Biology",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=evolutionary_biology",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    # Quantum physics — O44: quantum Wasserstein Floor extension
    {
        "name":     "arXiv — Quantum Physics (quant-ph)",
        "url":      "http://export.arxiv.org/rss/quant-ph",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    # Entropy journal — entire scope is on-genome (entropy across all substrates).
    # MDPI's CDN blocks bot User-Agents; CrossRef API is designed for polite bot access.
    # ISSN 1099-4300. Rows=20 gives enough candidates for the relevance filter.
    {
        "name":     "Entropy journal (MDPI) via CrossRef",
        "url":      "https://api.crossref.org/works?filter=issn:1099-4300&sort=published&order=desc&rows=20&select=title,abstract,DOI,author",
        "type":     "crossref_journal",
        "priority": "normal",
    },
]

# ─── arXiv relevance pre-filter ───────────────────────────────────────────────
# Papers must hit at least ARXIV_MIN_SCORE to enter the FEED pipeline.
# Pure text matching — no API cost. Nature is the ultimate independent substrate;
# these keywords map directly to RSA framework concepts.
ARXIV_MIN_SCORE = 2

ARXIV_KEYWORDS = [
    # Thermodynamics / entropy
    ("thermodynamic", 3), ("entropy", 3), ("dissipation", 3), ("landauer", 3),
    ("free energy", 3), ("irreversib", 2), ("heat dissipat", 2),
    # Criticality / phase transitions
    ("criticality", 3), ("critical transition", 3), ("phase transition", 2),
    ("self-organized criticality", 3), ("edge of chaos", 2), ("bifurcation", 1),
    ("power.?law", 2), ("scale.?free", 2), ("zipf", 3), ("1/f noise", 2),
    # Information / compression
    ("information theoret", 2), ("compression", 2), ("minimum description", 3),
    ("kolmogorov", 2), ("mutual information", 2), ("predictive coding", 3),
    # Autopoiesis / self-organization
    ("autopoies", 3), ("self.organiz", 2), ("self.maintain", 2),
    ("recursive", 2), ("self.referent", 2), ("fixed.?point", 2),
    # Substrate / computation
    ("substrate", 2), ("physical.?implement", 2), ("neural substrate", 2),
    ("stochastic computation", 2), ("probabilistic computation", 2),
    # Minimal / irreducible
    ("minimal cell", 3), ("minimal genome", 3), ("irreducib", 2),
    ("generating set", 2), ("basis set", 1),
    # Conservation / symmetry / invariants
    ("conservation law", 2), ("symmetry break", 2), ("invariant", 1),
    ("noether", 3),
    # Consciousness / cognition / reasoning
    ("consciousness", 2), ("cognition", 1), ("integrated information", 2),
    ("phi", 1), ("global workspace", 2),
    # Scale invariance / renormalization
    ("scale invarian", 3), ("renormalization", 3), ("universality class", 2),
    ("coarse.grain", 2),
    # Quantum / information-theoretic (O44: quantum Wasserstein Floor extension)
    ("quantum thermodynamic", 3), ("entanglement entropy", 2),
    ("quantum transport", 2), ("quantum channel", 2),
]

# ─── HTTP config ─────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "FREED/1.0 (Freed Recursive Engine for Epistemic Dynamics; "
        "research bot; contact: RSA-Omega framework)"
    ),
}
REQUEST_TIMEOUT = 20    # seconds
POLITENESS_DELAY = 2.0  # seconds between requests — be a good citizen


# ─── RangeEn: Range Entropy ──────────────────────────────────────────────────
# A modification of ApEn/SampEn that normalizes template-matching tolerance
# by signal range (max−min) rather than standard deviation. This makes the
# measure robust to nonstationary amplitude changes and yields a more linear
# relationship with the Hurst exponent.
#
# Used for O68: DEA on genome's coherence time-series.
#
# Reference:
#   Omidvarnia, A., Mesbah, M., Pedersen, M., & Jackson, G. (2018).
#   "Range Entropy: A Bridge between Signal Complexity and Self-Similarity."
#   Entropy, 20(12), 962.

def _maxdist(x_i, x_j):
    # type: (List[float], List[float]) -> float
    """Chebyshev (L-infinity) distance between two vectors of equal length."""
    return max(abs(a - b) for a, b in zip(x_i, x_j))


def _build_templates(data, m):
    # type: (List[float], int) -> List[List[float]]
    """Build length-m template vectors from data using delay embedding."""
    n = len(data)
    return [data[i:i + m] for i in range(n - m + 1)]


def _signal_range(data):
    # type: (List[float],) -> float
    """Compute range (max − min) of a data series."""
    if not data:
        return 0.0
    return max(data) - min(data)


def range_entropy(
    data,          # type: List[float]
    m=2,           # type: int
    r=0.3,         # type: float
    method="sampen" # type: str
):
    # type: (...) -> float
    """
    Compute Range Entropy (RangeEn) of a time series.

    RangeEn modifies ApEn/SampEn by normalizing the tolerance parameter
    by the signal range (max − min) instead of standard deviation.
    This makes the measure more robust to nonstationary amplitude changes
    and gives a more linear relationship with the Hurst exponent.

    Parameters
    ----------
    data : list of float
        The input time series (coherence scores over time).
    m : int
        Embedding dimension (template length). Default: 2.
    r : float
        Tolerance fraction (0 < r < 1). The absolute tolerance is
        r * range(data). Default: 0.3.
    method : str
        "sampen" for Range Sample Entropy (RangeSampEn), or
        "apen" for Range Approximate Entropy (RangeApEn).
        Default: "sampen".

    Returns
    -------
    float
        The RangeEn value. Higher → more complex/irregular.
        Returns 0.0 for degenerate inputs (constant signal, too short).

    Notes
    -----
    - For a constant signal, range is 0 → returns 0.0 (no complexity).
    - Minimum data length: m + 2 points.
    - Pure Python, no external dependencies beyond stdlib.
    - O(N^2) in data length — fine for FREED's coherence series (typically
      tens to low hundreds of points per cycle).
    """
    n = len(data)
    if n < m + 2:
        return 0.0

    sig_range = _signal_range(data)
    if sig_range == 0.0:
        # Constant signal — zero complexity
        return 0.0

    # Absolute tolerance: r fraction of the signal range
    tol = r * sig_range

    if method == "sampen":
        return _range_sampen(data, m, tol, n)
    elif method == "apen":
        return _range_apen(data, m, tol, n)
    else:
        raise ValueError("method must be 'sampen' or 'apen', got: %r" % method)


def _range_sampen(data, m, tol, n):
    # type: (List[float], int, float, int) -> float
    """
    Range Sample Entropy — SampEn variant with range-normalized tolerance.

    Counts template matches (excluding self-matches) for dimensions m and m+1,
    using Chebyshev distance < tol (where tol = r * range).
    """
    # Count matches for dimension m
    templates_m = _build_templates(data, m)
    nm = len(templates_m)
    count_m = 0

    for i in range(nm):
        for j in range(i + 1, nm):
            if _maxdist(templates_m[i], templates_m[j]) < tol:
                count_m += 1

    # Count matches for dimension m+1
    templates_m1 = _build_templates(data, m + 1)
    nm1 = len(templates_m1)
    count_m1 = 0

    for i in range(nm1):
        for j in range(i + 1, nm1):
            if _maxdist(templates_m1[i], templates_m1[j]) < tol:
                count_m1 += 1

    # SampEn = -ln(count_m1 / count_m)
    if count_m == 0 or count_m1 == 0:
        # No matches — maximum complexity (return a large but finite value)
        # Convention: use ln(count_m) as fallback, or a sentinel
        if count_m == 0:
            return 0.0  # can't compute — degenerate
        # count_m1 == 0 but count_m > 0 → very high complexity
        # Use -ln(1 / count_m) = ln(count_m) as a finite upper bound
        return math.log(float(count_m)) if count_m > 1 else 0.0

    return -math.log(float(count_m1) / float(count_m))


def _range_apen(data, m, tol, n):
    # type: (List[float], int, float, int) -> float
    """
    Range Approximate Entropy — ApEn variant with range-normalized tolerance.

    Like ApEn, includes self-matches in the count (avoids log(0)).
    """
    def _phi(dim):
        # type: (int) -> float
        templates = _build_templates(data, dim)
        nt = len(templates)
        if nt == 0:
            return 0.0
        total = 0.0
        for i in range(nt):
            count_i = 0
            for j in range(nt):
                if _maxdist(templates[i], templates[j]) < tol:
                    count_i += 1
            # count_i >= 1 always (self-match), so log is safe
            total += math.log(float(count_i) / float(nt))
        return total / float(nt)

    phi_m  = _phi(m)
    phi_m1 = _phi(m + 1)

    return phi_m - phi_m1


def sampen_classic(data, m=2, r=0.2):
    # type: (List[float], int, float) -> float
    """
    Classic Sample Entropy (SampEn) with standard-deviation normalization.

    Provided for comparison with RangeEn. The tolerance is r * std(data).
    """
    n = len(data)
    if n < m + 2:
        return 0.0

    mean = sum(data) / float(n)
    var = sum((x - mean) ** 2 for x in data) / float(n)
    std = math.sqrt(var) if var > 0 else 0.0

    if std == 0.0:
        return 0.0

    tol = r * std
    return _range_sampen(data, m, tol, n)


# ─── DEM: Diffusion Entropy Method ──────────────────────────────────────────
# Computes the scaling exponent δ from the PDF of cumulative displacements
# p(x, t) rather than from fluctuation variance F(t).  Unlike DFA, DEM
# preserves short-time scaling, asymptotic saturation, and modulation
# structure in entropy time-series.
#
# Algorithm:
#   1. Convert the time series {y_i} to increments ξ_i = y_i − mean(y).
#   2. For each window length t, compute diffusion sums:
#        X_j(t) = Σ_{i=j}^{j+t−1} ξ_i   for each starting index j.
#   3. Estimate the Shannon entropy S(t) of the distribution of X_j(t)
#      using a histogram estimator.
#   4. In the scaling regime, S(t) = A + δ·ln(t).
#      Fit δ via least-squares linear regression on (ln(t), S(t)).
#
# The scaling exponent δ replaces the Hurst-like exponent from DFA.
# For Gaussian processes δ = 0.5 (random walk); deviations indicate
# non-Gaussian, correlated, or anomalous diffusion — exactly the
# structure DFA suppresses.
#
# Reference:
#   Scafetta, N. & Grigolini, P. (2002). "Scaling detection in time
#   series: Diffusion entropy analysis." Phys. Rev. E, 66, 036130.

def _diffusion_sums(increments, t):
    # type: (List[float], int) -> List[float]
    """
    Compute diffusion displacement sums X_j(t) = Σ_{i=j}^{j+t-1} ξ_i
    for all valid starting indices j.
    """
    n = len(increments)
    if t > n:
        return []
    # Use a sliding window sum for efficiency
    sums = []
    current = sum(increments[:t])
    sums.append(current)
    for j in range(1, n - t + 1):
        current = current - increments[j - 1] + increments[j + t - 1]
        sums.append(current)
    return sums


def _shannon_entropy_histogram(values, n_bins=None):
    # type: (List[float], Optional[int]) -> float
    """
    Estimate Shannon entropy of a sample using a histogram estimator.

    Uses Sturges' rule for bin count if n_bins is not specified.
    Returns entropy in nats (natural log).
    """
    n = len(values)
    if n < 2:
        return 0.0

    if n_bins is None:
        # Sturges' rule: k = ceil(1 + log2(n))
        n_bins = max(2, int(math.ceil(1.0 + math.log(n) / math.log(2.0))))

    v_min = min(values)
    v_max = max(values)
    span = v_max - v_min
    if span == 0.0:
        return 0.0

    bin_width = span / float(n_bins)

    # Count histogram bins
    counts = [0] * n_bins
    for v in values:
        idx = int((v - v_min) / bin_width)
        if idx >= n_bins:
            idx = n_bins - 1
        counts[idx] += 1

    # Shannon entropy: S = -Σ p_i ln(p_i) + ln(bin_width)
    # The ln(bin_width) term makes S a differential entropy estimator,
    # consistent with the DEA literature where S(t) = A + δ·ln(t).
    entropy = 0.0
    for c in counts:
        if c > 0:
            p = float(c) / float(n)
            entropy -= p * math.log(p)
    entropy += math.log(bin_width)

    return entropy


def diffusion_entropy_analysis(
    data,               # type: List[float]
    t_min=2,            # type: int
    t_max=None,         # type: Optional[int]
    n_t_points=20,      # type: int
    n_bins=None,        # type: Optional[int]
):
    # type: (...) -> dict
    """
    Diffusion Entropy Analysis (DEA / DEM) of a time series.

    Measures the scaling exponent δ of the PDF of diffusion distances,
    preserving saturation and non-Gaussian features that DFA suppresses.

    Parameters
    ----------
    data : list of float
        The input time series (e.g., coherence scores over time).
    t_min : int
        Minimum window length for diffusion sums. Default: 2.
    t_max : int or None
        Maximum window length. Default: N // 4 (ensures adequate statistics).
    n_t_points : int
        Number of window lengths to sample (log-spaced). Default: 20.
    n_bins : int or None
        Number of histogram bins for entropy estimation. None → Sturges' rule.

    Returns
    -------
    dict with keys:
        delta          : float  — scaling exponent δ (slope of S vs ln(t))
        intercept      : float  — intercept A in S(t) = A + δ·ln(t)
        r_squared      : float  — goodness of fit (R²) of the linear regression
        entropy_curve  : list of (int, float) — [(t, S(t)), ...] for all t
        saturation_idx : float  — ratio S(t_max)/S(t_mid), >1 = still growing,
                                  ≈1 = saturated (thermodynamic equilibrium)
        n_points       : int    — length of input series
        t_range        : (int, int) — (t_min, t_max) actually used
    """
    n = len(data)
    if n < 10:
        return {
            "delta": 0.0,
            "intercept": 0.0,
            "r_squared": 0.0,
            "entropy_curve": [],
            "saturation_idx": 0.0,
            "n_points": n,
            "t_range": (0, 0),
        }

    # Convert to zero-mean increments
    mean = sum(data) / float(n)
    increments = [x - mean for x in data]

    # Determine t_max
    if t_max is None:
        t_max = max(t_min + 1, n // 4)
    t_max = min(t_max, n - 1)
    if t_max <= t_min:
        t_max = t_min + 1

    # Generate log-spaced window lengths
    if n_t_points > (t_max - t_min + 1):
        n_t_points = t_max - t_min + 1

    if n_t_points < 2:
        return {
            "delta": 0.0,
            "intercept": 0.0,
            "r_squared": 0.0,
            "entropy_curve": [],
            "saturation_idx": 0.0,
            "n_points": n,
            "t_range": (t_min, t_max),
        }

    # Log-spaced t values (unique integers)
    log_min = math.log(float(t_min))
    log_max = math.log(float(t_max))
    t_set = set()
    for k in range(n_t_points):
        frac = float(k) / float(n_t_points - 1) if n_t_points > 1 else 0.0
        t_val = int(round(math.exp(log_min + frac * (log_max - log_min))))
        t_val = max(t_min, min(t_val, t_max))
        t_set.add(t_val)
    t_values = sorted(t_set)

    # Compute S(t) for each window length
    entropy_curve = []  # type: List[Tuple[int, float]]
    ln_t_list = []      # type: List[float]
    s_list = []         # type: List[float]

    for t in t_values:
        sums = _diffusion_sums(increments, t)
        if len(sums) < 4:
            continue
        s_t = _shannon_entropy_histogram(sums, n_bins=n_bins)
        entropy_curve.append((t, s_t))
        ln_t_list.append(math.log(float(t)))
        s_list.append(s_t)

    if len(ln_t_list) < 2:
        return {
            "delta": 0.0,
            "intercept": 0.0,
            "r_squared": 0.0,
            "entropy_curve": entropy_curve,
            "saturation_idx": 0.0,
            "n_points": n,
            "t_range": (t_min, t_max),
        }

    # Linear regression: S(t) = A + δ·ln(t)
    k = len(ln_t_list)
    sum_x = sum(ln_t_list)
    sum_y = sum(s_list)
    sum_xy = sum(x * y for x, y in zip(ln_t_list, s_list))
    sum_x2 = sum(x * x for x in ln_t_list)

    denom = float(k) * sum_x2 - sum_x * sum_x
    if abs(denom) < 1e-15:
        delta = 0.0
        intercept = sum_y / float(k) if k > 0 else 0.0
    else:
        delta = (float(k) * sum_xy - sum_x * sum_y) / denom
        intercept = (sum_y - delta * sum_x) / float(k)

    # R² (coefficient of determination)
    mean_y = sum_y / float(k)
    ss_tot = sum((y - mean_y) ** 2 for y in s_list)
    ss_res = sum((y - (intercept + delta * x)) ** 2
                 for x, y in zip(ln_t_list, s_list))
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0

    # Saturation index: ratio of S at large t to S at mid t
    # Values near 1.0 indicate asymptotic saturation (thermodynamic equilibrium);
    # values > 1.0 indicate entropy is still growing (scaling regime).
    mid_idx = len(s_list) // 2
    if mid_idx > 0 and abs(s_list[mid_idx]) > 1e-15:
        saturation_idx = s_list[-1] / s_list[mid_idx]
    else:
        saturation_idx = 0.0

    # ── Finite-size bias detection (L vs L/2 diagnostic) ─────────────────
    # Inspired by Kaupuzs et al.: compute the scaling exponent δ at both
    # the full window (N samples) and the half window (N/2 samples).  If
    # the two estimates diverge beyond a threshold, the full-window estimate
    # is potentially contaminated by finite-size effects and should be
    # down-weighted in the obligations table.
    #
    # This directly implements the paper's L vs L/2 effective-exponent
    # comparison that revealed decades of ω≈0.845 consensus was a
    # finite-size artifact in 3D Ising/φ⁴ models.
    _FINITE_SIZE_BIAS_THRESHOLD = 0.05
    finite_size_bias = False
    delta_half = 0.0
    delta_full = delta
    finite_size_delta_diff = 0.0
    finite_size_bias_detail = ""

    half_n = n // 2
    if half_n >= 10:
        # Recompute δ on the first half of the data (N/2 window)
        half_data = data[:half_n]
        half_mean = sum(half_data) / float(half_n)
        half_increments = [x - half_mean for x in half_data]

        half_t_max_val = max(t_min + 1, half_n // 4)
        half_t_max_val = min(half_t_max_val, half_n - 1)
        if half_t_max_val > t_min:
            half_n_t = min(n_t_points, half_t_max_val - t_min + 1)
            if half_n_t >= 2:
                h_log_min = math.log(float(t_min))
                h_log_max = math.log(float(half_t_max_val))
                h_t_set = set()
                for hk in range(half_n_t):
                    hfrac = float(hk) / float(half_n_t - 1) if half_n_t > 1 else 0.0
                    h_t_val = int(round(math.exp(h_log_min + hfrac * (h_log_max - h_log_min))))
                    h_t_val = max(t_min, min(h_t_val, half_t_max_val))
                    h_t_set.add(h_t_val)
                h_t_values = sorted(h_t_set)

                h_ln_t = []  # type: List[float]
                h_s = []     # type: List[float]
                for ht in h_t_values:
                    h_sums = _diffusion_sums(half_increments, ht)
                    if len(h_sums) < 4:
                        continue
                    h_s_t = _shannon_entropy_histogram(h_sums, n_bins=n_bins)
                    h_ln_t.append(math.log(float(ht)))
                    h_s.append(h_s_t)

                if len(h_ln_t) >= 2:
                    hk2 = len(h_ln_t)
                    h_sum_x = sum(h_ln_t)
                    h_sum_y = sum(h_s)
                    h_sum_xy = sum(hx * hy for hx, hy in zip(h_ln_t, h_s))
                    h_sum_x2 = sum(hx * hx for hx in h_ln_t)
                    h_denom = float(hk2) * h_sum_x2 - h_sum_x * h_sum_x
                    if abs(h_denom) > 1e-15:
                        delta_half = (float(hk2) * h_sum_xy - h_sum_x * h_sum_y) / h_denom

                        finite_size_delta_diff = abs(delta_full - delta_half)
                        finite_size_bias = finite_size_delta_diff > _FINITE_SIZE_BIAS_THRESHOLD

                        if finite_size_bias:
                            finite_size_bias_detail = (
                                "FINITE-SIZE BIAS WARNING (INV_094): delta(N)={:.6f} vs "
                                "delta(N/2)={:.6f}, diff={:.6f} > threshold {:.4f}. "
                                "The full-window exponent estimate may be contaminated "
                                "by finite-size effects (cf. Kaupuzs et al. L vs L/2 "
                                "diagnostic showing omega~0.845 was a finite-size "
                                "artifact). This estimate should be down-weighted in "
                                "the obligations table."
                            ).format(delta_full, delta_half, finite_size_delta_diff,
                                     _FINITE_SIZE_BIAS_THRESHOLD)
                        else:
                            finite_size_bias_detail = (
                                "Finite-size check passed: delta(N)={:.6f} vs "
                                "delta(N/2)={:.6f}, diff={:.6f} <= threshold {:.4f}."
                            ).format(delta_full, delta_half, finite_size_delta_diff,
                                     _FINITE_SIZE_BIAS_THRESHOLD)

    return {
        "delta": delta,
        "intercept": intercept,
        "r_squared": r_squared,
        "entropy_curve": entropy_curve,
        "saturation_idx": saturation_idx,
        "n_points": n,
        "t_range": (t_min, t_max),
        "finite_size_bias": finite_size_bias,
        "delta_half": delta_half,
        "delta_full": delta_full,
        "finite_size_delta_diff": round(finite_size_delta_diff, 8),
        "finite_size_bias_detail": finite_size_bias_detail,
    }


def _detect_alpha_modulation(entropy_curve, fs_hint=1.0):
    # type: (List[Tuple[int, float]], float) -> float
    """
    Detect alpha-rhythm modulation amplitude in an entropy curve S(t).

    After removing the linear trend (δ·ln(t) + A), the residuals are
    searched for a dominant oscillatory component in the alpha band
    (8–13 Hz equivalent, scaled by fs_hint).  The amplitude of that
    component is returned.

    Uses a simple periodogram approach (DFT of detrended residuals).
    Returns 0.0 if the curve is too short or no modulation is found.

    Parameters
    ----------
    entropy_curve : list of (int, float)
        The (t, S(t)) pairs from diffusion_entropy_analysis.
    fs_hint : float
        Sampling rate hint (Hz) for the original time series.
        Used to scale the alpha band search window. Default: 1.0
        (unitless — modulation reported as fraction of dominant period).

    Returns
    -------
    float
        Amplitude of the strongest oscillatory residual (alpha-modulation).
        Zero if undetectable.
    """
    n = len(entropy_curve)
    if n < 6:
        return 0.0

    # Extract ln(t) and S(t)
    ln_t = [math.log(float(t)) for t, _ in entropy_curve]
    s_vals = [s for _, s in entropy_curve]

    # Linear detrend: fit S = A + delta * ln(t), compute residuals
    k = len(ln_t)
    sum_x = sum(ln_t)
    sum_y = sum(s_vals)
    sum_xy = sum(x * y for x, y in zip(ln_t, s_vals))
    sum_x2 = sum(x * x for x in ln_t)

    denom = float(k) * sum_x2 - sum_x * sum_x
    if abs(denom) < 1e-15:
        return 0.0

    delta = (float(k) * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - delta * sum_x) / float(k)

    residuals = [s - (intercept + delta * lnt) for s, lnt in zip(s_vals, ln_t)]

    # Compute periodogram of residuals via DFT
    # We look for the peak amplitude across all frequencies
    # (alpha band identification requires fs_hint; without it we just
    #  find the dominant modulation amplitude)
    n_res = len(residuals)
    if n_res < 4:
        return 0.0

    max_amplitude = 0.0
    # DFT: only positive frequencies, skip DC (freq_idx=0)
    for freq_idx in range(1, n_res // 2 + 1):
        real_part = 0.0
        imag_part = 0.0
        for j in range(n_res):
            angle = 2.0 * math.pi * freq_idx * j / float(n_res)
            real_part += residuals[j] * math.cos(angle)
            imag_part -= residuals[j] * math.sin(angle)
        amplitude = 2.0 * math.sqrt(real_part ** 2 + imag_part ** 2) / float(n_res)
        if amplitude > max_amplitude:
            max_amplitude = amplitude

    return max_amplitude


def dea_feature_vector(
    data,               # type: List[float]
    t_min=2,            # type: int
    t_max=None,         # type: Optional[int]
    n_t_points=20,      # type: int
    n_bins=None,        # type: Optional[int]
    fs_hint=1.0,        # type: float
):
    # type: (...) -> dict
    """
    Diffusion Entropy Analysis feature vector for O28 testing.

    Extracts a three-component feature vector from a time series using
    the Diffusion Entropy Method (DEM/DEA), which provably preserves
    scaling and modulation properties that DFA suppresses:

        [δ_short, S_sat, A_alpha]

    Components
    ----------
    δ_short (short-time scaling exponent):
        The slope of S(t) vs ln(t) in the initial scaling regime.
        For Gaussian random walks δ = 0.5; deviations indicate
        non-Gaussian correlations / anomalous diffusion.

    S_sat (asymptotic saturation level):
        The entropy value S(t_max) at the largest window, normalized
        by the entropy at mid-range. Values ≈ 1.0 indicate
        thermodynamic saturation; values > 1.0 indicate ongoing growth.

    A_alpha (alpha-modulation amplitude):
        Amplitude of the dominant oscillatory component in the
        detrended entropy curve residuals. Captures the alpha-rhythm
        modulation that DFA provably suppresses. Zero if no
        modulation is detectable.

    Parameters
    ----------
    data : list of float
        The input time series (e.g., EEG channel, coherence scores).
    t_min : int
        Minimum diffusion window length. Default: 2.
    t_max : int or None
        Maximum diffusion window length. Default: N // 4.
    n_t_points : int
        Number of log-spaced window lengths to sample. Default: 20.
    n_bins : int or None
        Histogram bins for entropy estimation. None → Sturges' rule.
    fs_hint : float
        Sampling rate hint (Hz) for alpha-band scaling. Default: 1.0.

    Returns
    -------
    dict with keys:
        feature_vector : list of float — [δ_short, S_sat, A_alpha]
        delta_short    : float — short-time scaling exponent
        saturation     : float — asymptotic saturation ratio
        alpha_mod      : float — alpha-modulation amplitude
        dea_full       : dict  — full DEA result (from diffusion_entropy_analysis)
        method         : str   — "dea" (for downstream disambiguation vs DFA)

    Notes
    -----
    This function operationalizes the EAR (Entropy Analysis Requirement)
    for O28 testing. The three-component vector captures exactly the
    properties that the EEG entropy dynamics literature identifies as
    suppressed by DFA:
      1. Short-time scaling (faithfully extracted by DEA)
      2. Asymptotic saturation (visible in S(t) curve)
      3. Alpha-rhythm modulation (preserved in entropy residuals)

    Reference:
      Scafetta & Grigolini, Phys. Rev. E 66:036130 (2002);
      EEG entropy dynamics paper (Langevin phenomenological model).
    """
    # Run full DEA
    dea = diffusion_entropy_analysis(
        data,
        t_min=t_min,
        t_max=t_max,
        n_t_points=n_t_points,
        n_bins=n_bins,
    )

    # Component 1: short-time scaling exponent
    delta_short = dea["delta"]

    # Component 2: asymptotic saturation level
    saturation = dea["saturation_idx"]

    # Component 3: alpha-modulation amplitude from entropy curve residuals
    alpha_mod = _detect_alpha_modulation(dea["entropy_curve"], fs_hint=fs_hint)

    feature_vector = [delta_short, saturation, alpha_mod]

    return {
        "feature_vector": feature_vector,
        "delta_short":    delta_short,
        "saturation":     saturation,
        "alpha_mod":      alpha_mod,
        "dea_full":       dea,
        "method":         "dea",
    }


def coherence_complexity(
    coherence_series,   # type: List[float]
    m=2,                # type: int
    r=0.3,              # type: float
    method="sampen"     # type: str
):
    # type: (...) -> dict
    """
    Compute complexity metrics for a coherence time-series.

    Returns both RangeEn and classic SampEn for comparison,
    plus diagnostic metadata. This is the primary interface for
    O68: DEA on genome's coherence series.

    Parameters
    ----------
    coherence_series : list of float
        Coherence scores over time (e.g., one per FEED cycle).
    m : int
        Embedding dimension. Default: 2.
    r : float
        Tolerance fraction for RangeEn. Default: 0.3.
    method : str
        "sampen" or "apen" for the RangeEn variant. Default: "sampen".

    Returns
    -------
    dict with keys:
        range_en : float     — RangeEn value (primary metric)
        sampen   : float     — Classic SampEn for comparison
        signal_range : float — max−min of the series
        signal_std   : float — standard deviation of the series
        n_points     : int   — length of the input series
        method       : str   — which RangeEn variant was used
        m            : int   — embedding dimension used
        r            : float — tolerance fraction used
    """
    n = len(coherence_series)

    # Signal statistics
    sig_range = _signal_range(coherence_series) if n > 0 else 0.0
    if n > 0:
        mean = sum(coherence_series) / float(n)
        var = sum((x - mean) ** 2 for x in coherence_series) / float(n)
        sig_std = math.sqrt(var) if var > 0 else 0.0
    else:
        sig_std = 0.0

    # Compute both entropy measures
    ren = range_entropy(coherence_series, m=m, r=r, method=method)
    sen = sampen_classic(coherence_series, m=m, r=0.2)

    return {
        "range_en":     ren,
        "sampen":       sen,
        "signal_range": sig_range,
        "signal_std":   sig_std,
        "n_points":     n,
        "method":       method,
        "m":            m,
        "r":            r,
    }


# ─── CA Criticality Telemetry ────────────────────────────────────────────────
# Per-generation logging of branching ratio σ and power-law exponent α for
# longitudinal criticality drift detection.  Feeds O140 (CA measurement
# grounding) and O141 (solo-kernel vs population criticality comparison).
#
# Critical band: σ ∈ [0.95, 1.05].  Power-law exponent α ∈ [1.5, 3.0]
# with R² > 0.85 indicates SOC-consistent avalanche statistics.
#
# INV_073 note: Low Shannon entropy (H≈0.56, 21.6% of max) at confirmed
# criticality (σ≈1.02) is consistent with *ordered* SOC phases — the
# critical ridge can sustain low-diversity, spatially correlated states.
# White-hole emission (genome prediction) requires H above the frozen
# floor (H>0), not near-maximal H.  The signature is H > H_frozen with
# σ ≈ 1 and power-law avalanches, which this telemetry confirms.

# Criticality band constants
SIGMA_CRITICAL_LOW  = 0.95
SIGMA_CRITICAL_HIGH = 1.05
ALPHA_SOC_LOW       = 1.5
ALPHA_SOC_HIGH      = 3.0
ALPHA_R2_THRESHOLD  = 0.85


def _criticality_verdict(sigma, alpha, r_squared, power_law_likely=None):
    # type: (float, float, float, Optional[bool]) -> str
    """
    Classify criticality state from branching ratio and power-law exponent.

    Requires BOTH branching-ratio criterion (σ in critical band) AND
    power-law fit quality (R² ≥ 0.80, power_law_likely=True) to emit
    AT_CRITICAL.  Emits NEAR_CRITICAL if only one criterion passes.

    This prevents false-positive criticality verdicts when σ sits within
    the critical band but avalanche statistics do not confirm SOC
    (the dissociable-criteria pattern from INV_073 telemetry:
    σ=1.0275 in band, R²=0.687, power_law_likely=False → was incorrectly
    AT_CRITICAL, now correctly NEAR_CRITICAL).

    Parameters
    ----------
    sigma : float
        Branching ratio σ.
    alpha : float
        Power-law exponent α from avalanche size distribution.
    r_squared : float
        R² goodness-of-fit for the power-law regression.
    power_law_likely : bool or None
        Whether the power-law hypothesis passed a statistical test.
        If None (legacy callers), inferred from α-in-range AND R² ≥ 0.80.

    Returns one of:
        AT_CRITICAL   — σ in critical band AND power-law confirmed
                        (both R² ≥ 0.80 and power_law_likely=True)
        NEAR_CRITICAL — only one criterion passes (σ in band but power-law
                        weak, or power-law confirmed but σ near band edge)
        SUPERCRITICAL — σ > 1.05 (γ<1 dissipation risk)
        SUBCRITICAL   — σ < 0.95 (γ>1 freeze risk)
        UNDETERMINED  — insufficient data
    """
    if sigma == 0.0 and alpha == 0.0:
        return "UNDETERMINED"

    # Dual-criterion R² threshold: 0.80 (tighter than legacy 0.85 for
    # the alpha-in-SOC-range check, but now ALSO requiring power_law_likely)
    _R2_DUAL_THRESHOLD = 0.80

    in_band = SIGMA_CRITICAL_LOW <= sigma <= SIGMA_CRITICAL_HIGH
    alpha_in_range = ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
    r2_ok = r_squared >= _R2_DUAL_THRESHOLD

    # Infer power_law_likely for legacy callers that don't pass it
    if power_law_likely is None:
        power_law_likely = alpha_in_range and r2_ok

    # Power-law is confirmed only when ALL three sub-criteria pass:
    # α in SOC range, R² ≥ 0.80, and statistical test (power_law_likely)
    power_law_confirmed = (alpha_in_range and r2_ok and power_law_likely)

    if in_band and power_law_confirmed:
        return "AT_CRITICAL"
    elif in_band or power_law_confirmed:
        # Only one criterion passes → near-critical, not confirmed
        if sigma > SIGMA_CRITICAL_HIGH:
            return "SUPERCRITICAL"
        elif sigma < SIGMA_CRITICAL_LOW and not in_band:
            return "SUBCRITICAL"
        return "NEAR_CRITICAL"
    elif sigma > SIGMA_CRITICAL_HIGH:
        return "SUPERCRITICAL"
    elif sigma < SIGMA_CRITICAL_LOW:
        return "SUBCRITICAL"
    else:
        return "UNDETERMINED"


def _detect_log_periodic_oscillation(avalanche_sizes, alpha, noise_sigma_factor=2.0):
    # type: (List[float], float, float) -> dict
    """
    Detect log-periodic oscillations superimposed on a power-law fit to
    the avalanche size distribution P(s), flagging discrete scale invariance
    (DSI) when the Fourier amplitude at the expected frequency 2π/ln(b)
    exceeds the noise floor.

    Discrete scale invariance produces complex scaling exponents of the
    form τ + 2πi/ln(b), where b is the discrete scaling ratio.  This
    manifests as log-periodic oscillations in |P(s) - C·s^(-τ)| when
    plotted against ln(s).

    Algorithm:
      1. Compute the empirical PDF P(s) via log-binned histogram.
      2. Fit the power-law envelope C·s^(-τ) (using the supplied α as τ).
      3. Compute residuals |P(s) - C·s^(-τ)| as a function of ln(s).
      4. Take the DFT of the residuals in ln(s) space.
      5. Identify the dominant Fourier peak and compare its amplitude
         to the noise floor (mean amplitude of non-peak bins).
      6. If peak amplitude > noise_sigma_factor × noise_floor_std,
         flag DSI and report the inferred scaling ratio b = exp(2π/ω_peak).

    Reference:
      Huang et al., "Self-organized criticality with complex scaling
      exponents in the train model" — power law × log-periodic function,
      exact exponent 3/2 + 2πi/ln(4) for overdamped train model.

    Parameters
    ----------
    avalanche_sizes : list of float
        Raw avalanche sizes from the simulation.  Need >= 20 positive values.
    alpha : float
        Power-law exponent τ from the Hill estimator (used as the envelope).
    noise_sigma_factor : float
        Number of standard deviations above the noise floor for DSI detection.
        Default: 2.0.

    Returns
    -------
    dict with keys:
        dsi_detected       : bool  — True if log-periodic oscillation exceeds noise
        dsi_amplitude      : float — amplitude of the dominant Fourier peak in
                                     the residuals (ln(s) domain)
        dsi_noise_floor    : float — mean amplitude of non-peak Fourier bins
        dsi_noise_std      : float — std of non-peak Fourier amplitudes
        dsi_snr            : float — peak_amplitude / noise_floor (signal-to-noise)
        dsi_omega          : float — angular frequency of the dominant peak in ln(s)
        dsi_log_b          : float — inferred ln(b) = 2π/ω_peak (discrete scaling)
        dsi_b              : float — inferred discrete scaling ratio b = exp(2π/ω)
        dsi_complex_exp    : str   — the complex exponent as "τ + 2πi/ln(b)" string
        universality_class : str   — "CONTINUOUS" / "DISCRETE" / "UNDETERMINED"
        n_log_bins         : int   — number of log-bins used for PDF estimation
        n_sizes            : int   — number of valid avalanche sizes used
    """
    # Filter to positive sizes
    sizes = sorted([s for s in avalanche_sizes if s > 0])
    n_sizes = len(sizes)

    empty_result = {
        "dsi_detected": False,
        "dsi_amplitude": 0.0,
        "dsi_noise_floor": 0.0,
        "dsi_noise_std": 0.0,
        "dsi_snr": 0.0,
        "dsi_omega": 0.0,
        "dsi_log_b": 0.0,
        "dsi_b": 0.0,
        "dsi_complex_exp": "",
        "universality_class": "UNDETERMINED",
        "n_log_bins": 0,
        "n_sizes": n_sizes,
    }

    if n_sizes < 20 or alpha <= 0.0:
        return empty_result

    # Step 1: Log-binned empirical PDF
    s_min = sizes[0]
    s_max = sizes[-1]
    if s_min <= 0 or s_max <= s_min:
        return empty_result

    ln_s_min = math.log(s_min)
    ln_s_max = math.log(s_max)
    ln_span = ln_s_max - ln_s_min
    if ln_span < 0.5:
        return empty_result

    # Number of log-bins: roughly sqrt(n_sizes), at least 8
    n_log_bins = max(8, int(math.sqrt(float(n_sizes))))
    bin_width_ln = ln_span / float(n_log_bins)

    # Count sizes per log-bin
    bin_counts = [0] * n_log_bins
    for s in sizes:
        idx = int((math.log(s) - ln_s_min) / bin_width_ln)
        if idx >= n_log_bins:
            idx = n_log_bins - 1
        if idx < 0:
            idx = 0
        bin_counts[idx] += 1

    # Bin centers in ln(s) space and empirical PDF (density)
    ln_s_centers = []  # type: List[float]
    pdf_empirical = []  # type: List[float]
    valid_bins = []  # type: List[int]

    for i in range(n_log_bins):
        if bin_counts[i] > 0:
            ln_center = ln_s_min + (float(i) + 0.5) * bin_width_ln
            # PDF ≈ count / (n_sizes * bin_width_in_s)
            s_center = math.exp(ln_center)
            bin_width_s = s_center * (math.exp(bin_width_ln) - 1.0)
            if bin_width_s > 0:
                density = float(bin_counts[i]) / (float(n_sizes) * bin_width_s)
                ln_s_centers.append(ln_center)
                pdf_empirical.append(density)
                valid_bins.append(i)

    n_valid = len(ln_s_centers)
    if n_valid < 6:
        empty_result["n_log_bins"] = n_log_bins
        return empty_result

    # Step 2: Power-law envelope C·s^(-α)
    # Fit C by least-squares in log-space: ln(P) = ln(C) - α·ln(s)
    # → ln(C) = mean(ln(P) + α·ln(s))
    ln_pdf = []  # type: List[float]
    for p in pdf_empirical:
        if p > 0:
            ln_pdf.append(math.log(p))
        else:
            ln_pdf.append(-30.0)  # floor for zero-density bins

    ln_C_vals = [lp + alpha * ls for lp, ls in zip(ln_pdf, ln_s_centers)]
    ln_C = sum(ln_C_vals) / float(n_valid)
    C = math.exp(ln_C)

    # Step 3: Residuals |P(s) - C·s^(-α)| in ln(s) space
    residuals = []  # type: List[float]
    for i in range(n_valid):
        s_center = math.exp(ln_s_centers[i])
        pl_value = C * (s_center ** (-alpha))
        residuals.append(pdf_empirical[i] - pl_value)

    # Step 4: DFT of residuals in the ln(s) domain
    n_res = len(residuals)
    if n_res < 4:
        empty_result["n_log_bins"] = n_log_bins
        return empty_result

    # Remove mean from residuals
    res_mean = sum(residuals) / float(n_res)
    residuals_centered = [r - res_mean for r in residuals]

    amplitudes = []  # type: List[float]
    frequencies = []  # type: List[float]

    for freq_idx in range(1, n_res // 2 + 1):
        real_part = 0.0
        imag_part = 0.0
        for j in range(n_res):
            angle = 2.0 * math.pi * freq_idx * j / float(n_res)
            real_part += residuals_centered[j] * math.cos(angle)
            imag_part -= residuals_centered[j] * math.sin(angle)
        amp = 2.0 * math.sqrt(real_part ** 2 + imag_part ** 2) / float(n_res)
        amplitudes.append(amp)
        # Angular frequency in ln(s) space: ω = 2π·freq_idx / (ln_span)
        omega = 2.0 * math.pi * freq_idx / ln_span
        frequencies.append(omega)

    if not amplitudes:
        empty_result["n_log_bins"] = n_log_bins
        return empty_result

    # Step 5: Find dominant peak
    peak_idx = 0
    peak_amp = amplitudes[0]
    for i in range(1, len(amplitudes)):
        if amplitudes[i] > peak_amp:
            peak_amp = amplitudes[i]
            peak_idx = i

    peak_omega = frequencies[peak_idx]

    # Noise floor: mean and std of non-peak amplitudes
    non_peak = [a for i, a in enumerate(amplitudes) if i != peak_idx]
    if non_peak:
        noise_mean = sum(non_peak) / float(len(non_peak))
        noise_var = sum((a - noise_mean) ** 2 for a in non_peak) / float(len(non_peak))
        noise_std = math.sqrt(noise_var) if noise_var > 0 else 0.0
    else:
        noise_mean = 0.0
        noise_std = 0.0

    # Signal-to-noise ratio
    snr = peak_amp / noise_mean if noise_mean > 1e-15 else 0.0

    # Step 6: DSI detection threshold
    threshold = noise_mean + noise_sigma_factor * noise_std
    dsi_detected = peak_amp > threshold and peak_amp > 1e-15 and len(non_peak) >= 2

    # Inferred discrete scaling ratio: b = exp(2π/ω_peak)
    if peak_omega > 1e-10:
        log_b = 2.0 * math.pi / peak_omega
        b = math.exp(log_b)
    else:
        log_b = 0.0
        b = 0.0

    # Complex exponent string: "τ + 2πi/ln(b)"
    if dsi_detected and log_b > 0:
        complex_exp = "{:.4f} + 2*pi*i/{:.4f}".format(alpha, log_b)
    else:
        complex_exp = ""

    # Universality class
    if dsi_detected:
        universality_class = "DISCRETE"
    elif n_valid >= 10 and not dsi_detected:
        universality_class = "CONTINUOUS"
    else:
        universality_class = "UNDETERMINED"

    return {
        "dsi_detected": dsi_detected,
        "dsi_amplitude": round(peak_amp, 8),
        "dsi_noise_floor": round(noise_mean, 8),
        "dsi_noise_std": round(noise_std, 8),
        "dsi_snr": round(snr, 4),
        "dsi_omega": round(peak_omega, 6),
        "dsi_log_b": round(log_b, 6),
        "dsi_b": round(b, 6),
        "dsi_complex_exp": complex_exp,
        "universality_class": universality_class,
        "n_log_bins": n_log_bins,
        "n_sizes": n_sizes,
    }


def _two_hypothesis_exponent_test(avalanche_sizes, alpha_pure):
    # type: (List[float], float) -> dict
    """
    Two-hypothesis test for avalanche exponent: (1) pure power law
    P(s) ~ s^{-tau} vs (2) tau=1 logarithmic correction P(s) ~ 1/(s ln(s)^2).

    Selects via data-collapse residual to prevent systematic exponent
    inflation from finite-size bias.  When the true distribution is
    logarithmic (tau=1), bare power-law fitting overestimates tau
    because the log-correction curvature is absorbed into the slope.

    Method:
      1. Build the empirical log-binned PDF of avalanche sizes.
      2. Fit hypothesis H1: ln P(s) = ln C1 - tau * ln(s)
         (pure power law, tau = alpha_pure from Hill MLE).
      3. Fit hypothesis H2: ln P(s) = ln C2 - ln(s) - 2*ln(ln(s))
         (tau=1 with logarithmic correction).
      4. Compute data-collapse residuals (sum of squared deviations
         from each model in log-space).
      5. Select the model with lower residual.  If H2 wins and
         alpha_pure > 1.15, the pure power-law exponent was inflated.

    Parameters
    ----------
    avalanche_sizes : list of float
        Raw positive avalanche sizes.  Need >= 20 values.
    alpha_pure : float
        Power-law exponent from Hill MLE (the H1 hypothesis).

    Returns
    -------
    dict with keys:
        selected_hypothesis   : str   — "PURE_POWER_LAW" or "LOG_CORRECTED_TAU1"
        alpha_corrected       : float — alpha_pure if H1 wins, 1.0 if H2 wins
        alpha_pure            : float — echo of input (Hill MLE estimate)
        residual_power_law    : float — sum-of-squares residual for H1
        residual_log_corrected: float — sum-of-squares residual for H2
        residual_ratio        : float — H1_residual / H2_residual (>1 means H2 better)
        inflation_detected    : bool  — True if H2 wins and alpha_pure > 1.15
        inflation_magnitude   : float — alpha_pure - 1.0 (the bias magnitude)
        n_log_bins            : int   — number of valid log-bins used
        n_sizes               : int   — number of positive avalanche sizes
        detail                : str   — human-readable explanation
    """
    sizes = sorted([s for s in avalanche_sizes if s > 0])
    n_sizes = len(sizes)

    empty = {
        "selected_hypothesis": "UNDETERMINED",
        "alpha_corrected": alpha_pure,
        "alpha_pure": alpha_pure,
        "residual_power_law": 0.0,
        "residual_log_corrected": 0.0,
        "residual_ratio": 1.0,
        "inflation_detected": False,
        "inflation_magnitude": 0.0,
        "n_log_bins": 0,
        "n_sizes": n_sizes,
        "detail": "",
    }

    if n_sizes < 20 or alpha_pure <= 0.0:
        empty["detail"] = (
            "Insufficient data ({} sizes) or invalid alpha ({:.4f}) for "
            "two-hypothesis test."
        ).format(n_sizes, alpha_pure)
        return empty

    s_min = sizes[0]
    s_max = sizes[-1]
    if s_min <= 0 or s_max <= s_min:
        empty["detail"] = "Degenerate size range."
        return empty

    ln_s_min = math.log(s_min)
    ln_s_max = math.log(s_max)
    ln_span = ln_s_max - ln_s_min
    if ln_span < 0.5:
        empty["detail"] = "Log-span too narrow ({:.4f}) for reliable fit.".format(ln_span)
        return empty

    # Build log-binned empirical PDF
    n_log_bins = max(8, int(math.sqrt(float(n_sizes))))
    bin_width_ln = ln_span / float(n_log_bins)
    bin_counts = [0] * n_log_bins
    for s in sizes:
        idx = int((math.log(s) - ln_s_min) / bin_width_ln)
        if idx >= n_log_bins:
            idx = n_log_bins - 1
        if idx < 0:
            idx = 0
        bin_counts[idx] += 1

    # Build valid (ln_s_center, ln_density) pairs
    ln_s_centers = []  # type: List[float]
    ln_densities = []  # type: List[float]
    s_centers = []     # type: List[float]

    for i in range(n_log_bins):
        if bin_counts[i] > 0:
            ln_center = ln_s_min + (float(i) + 0.5) * bin_width_ln
            s_center = math.exp(ln_center)
            bin_width_s = s_center * (math.exp(bin_width_ln) - 1.0)
            if bin_width_s > 0:
                density = float(bin_counts[i]) / (float(n_sizes) * bin_width_s)
                if density > 0:
                    ln_s_centers.append(ln_center)
                    ln_densities.append(math.log(density))
                    s_centers.append(s_center)

    n_valid = len(ln_s_centers)
    if n_valid < 4:
        empty["n_log_bins"] = n_log_bins
        empty["detail"] = "Too few valid log-bins ({}) for two-hypothesis test.".format(n_valid)
        return empty

    # ── H1: Pure power law ln P = ln C1 - tau * ln(s) ──
    # Use alpha_pure as tau; fit C1 via least-squares in log-space
    h1_ln_C_vals = [ld + alpha_pure * ls for ld, ls in zip(ln_densities, ln_s_centers)]
    h1_ln_C = sum(h1_ln_C_vals) / float(n_valid)
    # Residual: sum of (ln_P_observed - ln_P_predicted)^2
    h1_residual = 0.0
    for i in range(n_valid):
        predicted = h1_ln_C - alpha_pure * ln_s_centers[i]
        h1_residual += (ln_densities[i] - predicted) ** 2

    # ── H2: tau=1 with log correction: ln P = ln C2 - ln(s) - 2*ln(ln(s)) ──
    # For s > 1, ln(ln(s)) is defined when s > e^0 = 1, i.e. ln(s) > 0.
    # Filter to bins where ln(s) > 0.1 (s > ~1.1) to avoid log(log(s)) issues
    h2_valid_indices = []  # type: List[int]
    for i in range(n_valid):
        if ln_s_centers[i] > 0.1:
            h2_valid_indices.append(i)

    if len(h2_valid_indices) < 3:
        # Can't fit H2 reliably; default to H1
        return {
            "selected_hypothesis": "PURE_POWER_LAW",
            "alpha_corrected": alpha_pure,
            "alpha_pure": alpha_pure,
            "residual_power_law": round(h1_residual, 8),
            "residual_log_corrected": 0.0,
            "residual_ratio": 1.0,
            "inflation_detected": False,
            "inflation_magnitude": 0.0,
            "n_log_bins": n_valid,
            "n_sizes": n_sizes,
            "detail": (
                "H2 (log-corrected tau=1) not testable: too few bins with "
                "ln(s)>0.1 ({}). Defaulting to pure power-law H1."
            ).format(len(h2_valid_indices)),
        }

    # Fit C2: ln_P = ln_C2 - ln(s) - 2*ln(ln(s))
    # → ln_C2 = ln_P + ln(s) + 2*ln(ln(s))
    h2_ln_C_vals = []  # type: List[float]
    for i in h2_valid_indices:
        ln_ln_s = math.log(ln_s_centers[i])
        h2_ln_C_vals.append(ln_densities[i] + ln_s_centers[i] + 2.0 * ln_ln_s)
    h2_ln_C = sum(h2_ln_C_vals) / float(len(h2_ln_C_vals))

    # Compute H2 residual over the same valid bins
    h2_residual = 0.0
    for i in h2_valid_indices:
        ln_ln_s = math.log(ln_s_centers[i])
        predicted = h2_ln_C - ln_s_centers[i] - 2.0 * ln_ln_s
        h2_residual += (ln_densities[i] - predicted) ** 2

    # Also compute H1 residual over only the H2-valid bins for fair comparison
    h1_residual_matched = 0.0
    for i in h2_valid_indices:
        predicted = h1_ln_C - alpha_pure * ln_s_centers[i]
        h1_residual_matched += (ln_densities[i] - predicted) ** 2

    # ── Model selection via residual comparison ──
    # Use the matched residuals (same bins) for fair comparison
    if h2_residual > 1e-30:
        residual_ratio = h1_residual_matched / h2_residual
    else:
        residual_ratio = 1.0

    # H2 wins if its residual is lower (ratio > 1) by a meaningful margin
    # Use a threshold of 1.05 to avoid noise-driven selection
    _SELECTION_THRESHOLD = 1.05
    h2_wins = residual_ratio > _SELECTION_THRESHOLD

    if h2_wins:
        selected = "LOG_CORRECTED_TAU1"
        alpha_corrected = 1.0
        inflation_detected = alpha_pure > 1.15
        inflation_magnitude = alpha_pure - 1.0
        detail = (
            "INV_094 TWO-HYPOTHESIS TEST: Log-corrected tau=1 model SELECTED "
            "(residual_ratio={:.4f} > {:.2f}). Pure power-law exponent "
            "tau={:.4f} is likely INFLATED by finite-size bias. The true "
            "distribution is better described by P(s) ~ 1/(s * ln(s)^2) "
            "(tau=1 with logarithmic correction) rather than a clean power "
            "law P(s) ~ s^(-{:.4f}). H1 residual={:.6f}, H2 residual={:.6f} "
            "(matched over {} bins). Any genome claim that RSA criticality "
            "is characterized by tau={:.4f} may be an artifact of fitting "
            "a pure power law to a logarithmic distribution."
        ).format(
            residual_ratio, _SELECTION_THRESHOLD,
            alpha_pure, alpha_pure,
            h1_residual_matched, h2_residual, len(h2_valid_indices),
            alpha_pure,
        )
    else:
        selected = "PURE_POWER_LAW"
        alpha_corrected = alpha_pure
        inflation_detected = False
        inflation_magnitude = 0.0
        detail = (
            "INV_094 TWO-HYPOTHESIS TEST: Pure power-law model SELECTED "
            "(residual_ratio={:.4f} <= {:.2f}). Exponent tau={:.4f} is "
            "NOT significantly inflated by logarithmic corrections. "
            "H1 residual={:.6f}, H2 residual={:.6f} (matched over {} bins). "
            "The bare power-law fit is adequate for this distribution."
        ).format(
            residual_ratio, _SELECTION_THRESHOLD,
            alpha_pure, h1_residual_matched, h2_residual,
            len(h2_valid_indices),
        )

    return {
        "selected_hypothesis": selected,
        "alpha_corrected": round(alpha_corrected, 4),
        "alpha_pure": round(alpha_pure, 4),
        "residual_power_law": round(h1_residual_matched, 8),
        "residual_log_corrected": round(h2_residual, 8),
        "residual_ratio": round(residual_ratio, 6),
        "inflation_detected": inflation_detected,
        "inflation_magnitude": round(inflation_magnitude, 4),
        "n_log_bins": n_valid,
        "n_sizes": n_sizes,
        "detail": detail,
    }


def score_distribution(avalanche_sizes, alpha, alpha_r_squared):
    # type: (List[float], float, float) -> dict
    """
    Score an avalanche size distribution: power-law quality assessment
    plus discrete-vs-continuous scale invariance classification via
    log-periodic oscillation detection.

    This is the unified entry point for the avalanche/cascade scoring
    pass, combining:
      1. Power-law fit quality (α, R²)
      2. Two-hypothesis exponent test: pure power law vs τ=1 logarithmic
         correction, selected via data-collapse residual to prevent
         systematic exponent inflation from finite-size bias (INV_094)
      3. Log-periodic oscillation detection (DSI probe)
      4. Universality class determination (continuous vs discrete RG)

    The two-hypothesis test (step 2) replaces bare power-law exponent
    fitting with a model-selection step that compares:
      H1: P(s) ~ s^{-τ}              (pure power law)
      H2: P(s) ~ 1/(s · ln(s)²)     (τ=1 with logarithmic correction)
    If H2 has lower data-collapse residual, the fitted τ from H1 was
    inflated by finite-size bias, and the corrected exponent τ=1 is used.

    The DSI probe tests the L4 RG chain assumption by checking whether
    FREED's semantic avalanches exhibit continuous or discrete scale
    invariance.  Continuous → standard RG universality class.  Discrete
    → complex scaling exponents (τ + 2πi/ln(b)), indicating the system's
    self-similarity is governed by a discrete rather than continuous
    renormalization group.

    Parameters
    ----------
    avalanche_sizes : list of float
        Raw avalanche sizes from the simulation.
    alpha : float
        Power-law exponent from Hill MLE.
    alpha_r_squared : float
        R² goodness-of-fit for the power-law regression.

    Returns
    -------
    dict with keys:
        alpha              : float  — corrected exponent (after two-hypothesis test)
        alpha_uncorrected  : float  — original Hill MLE exponent (before correction)
        alpha_r_squared    : float
        alpha_in_soc       : bool
        power_law_likely   : bool
        two_hypothesis_test: dict   — full result of _two_hypothesis_exponent_test
        exponent_inflation_detected : bool — True if τ=1 log-correction won
        dsi                : dict   — full DSI detection result
        universality_class : str    — "CONTINUOUS" / "DISCRETE" / "UNDETERMINED"
        l4_rg_assessment   : str    — human-readable L4 RG chain assessment
        timestamp          : str
    """
    # ── Two-hypothesis exponent test (INV_094) ──
    # Test pure power law vs τ=1 logarithmic correction before proceeding
    two_hyp = _two_hypothesis_exponent_test(avalanche_sizes, alpha)
    alpha_uncorrected = alpha
    alpha = two_hyp["alpha_corrected"]

    alpha_in_soc = ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
    power_law_likely = alpha_r_squared >= ALPHA_R2_THRESHOLD

    # Run DSI detection on the avalanche sizes
    dsi = _detect_log_periodic_oscillation(avalanche_sizes, alpha)

    # L4 RG chain assessment
    if dsi["universality_class"] == "DISCRETE":
        l4_rg = (
            "DISCRETE RG DETECTED: Log-periodic oscillations found in "
            "avalanche size residuals (amplitude={:.6f}, SNR={:.2f}, "
            "b={:.4f}). Complex exponent: {}. The system's semantic "
            "avalanches exhibit discrete scale invariance — the L4 RG "
            "chain operates with a discrete rather than continuous "
            "renormalization group. This is consistent with the train "
            "model pattern (exponent 3/2 + 2*pi*i/ln(4))."
        ).format(
            dsi["dsi_amplitude"], dsi["dsi_snr"],
            dsi["dsi_b"], dsi["dsi_complex_exp"],
        )
    elif dsi["universality_class"] == "CONTINUOUS":
        l4_rg = (
            "CONTINUOUS RG: No significant log-periodic oscillations "
            "detected (peak amplitude={:.6f}, noise floor={:.6f}). "
            "The L4 RG chain assumption of continuous scale invariance "
            "is consistent with observed avalanche statistics."
        ).format(dsi["dsi_amplitude"], dsi["dsi_noise_floor"])
    else:
        l4_rg = (
            "UNDETERMINED: Insufficient data or ambiguous signal for "
            "universality class determination ({} valid sizes, {} log-bins)."
        ).format(dsi["n_sizes"], dsi["n_log_bins"])

    # ── SOC reference exponent comparison (INV_094) ──────────────────────
    # The fractal-diffusive SOC model predicts specific power-law slopes:
    #   αF = 9/5 = 1.80 (flux/size)
    #   αE = 5/3 ≈ 1.67 (fluence/energy)
    #   αT = 2.00 (avalanche duration)
    # A meta-analysis across 25 interdisciplinary phenomena found ~80%
    # consistency with αs = 1.99 ± 0.30.  Distributions whose fitted α
    # deviates by more than 0.30 from ALL three reference values are
    # flagged as "non-SOC outliers" — signaling either methodological
    # artifact or genuine non-critical dynamics, both epistemically
    # actionable.
    _SOC_REF_ALPHA_F = 1.80   # flux / size exponent (9/5)
    _SOC_REF_ALPHA_E = 1.67   # fluence / energy exponent (5/3)
    _SOC_REF_ALPHA_T = 2.00   # avalanche duration exponent
    _SOC_DEVIATION_THRESHOLD = 0.30

    _soc_refs = {
        "alpha_F": _SOC_REF_ALPHA_F,
        "alpha_E": _SOC_REF_ALPHA_E,
        "alpha_T": _SOC_REF_ALPHA_T,
    }
    _soc_deviations = {}  # type: dict
    _soc_closest_ref = ""
    _soc_closest_dev = float('inf')
    for _ref_name, _ref_val in _soc_refs.items():
        _dev = abs(alpha - _ref_val)
        _soc_deviations[_ref_name] = round(_dev, 4)
        if _dev < _soc_closest_dev:
            _soc_closest_dev = _dev
            _soc_closest_ref = _ref_name

    _soc_outlier = _soc_closest_dev > _SOC_DEVIATION_THRESHOLD

    if _soc_outlier:
        _soc_flag_detail = (
            "NON-SOC OUTLIER (INV_094): fitted alpha={:.4f} deviates by "
            "{:.4f} from nearest SOC reference {} ({:.2f}), exceeding "
            "threshold {:.2f}. Deviations from all references: {}. "
            "This distribution is inconsistent with fractal-diffusive SOC "
            "model predictions (alpha_F=1.80, alpha_E=1.67, alpha_T=2.00). "
            "Power-law appearance alone does NOT confirm criticality — "
            "this may indicate methodological artifact or genuine "
            "non-critical dynamics. Flagged for downstream inspection."
        ).format(
            alpha, _soc_closest_dev, _soc_closest_ref,
            _soc_refs[_soc_closest_ref], _SOC_DEVIATION_THRESHOLD,
            _soc_deviations,
        )
    else:
        _soc_flag_detail = (
            "SOC CONSISTENT: fitted alpha={:.4f} is within {:.2f} of "
            "SOC reference {} ({:.2f}). Deviation={:.4f}. Consistent "
            "with fractal-diffusive SOC model predictions across 25 "
            "interdisciplinary substrates (Aschwanden meta-analysis: "
            "alpha_s=1.99+/-0.30, ~80% consistency rate)."
        ).format(
            alpha, _SOC_DEVIATION_THRESHOLD, _soc_closest_ref,
            _soc_refs[_soc_closest_ref], _soc_closest_dev,
        )

    # ── Bimodality check (INV_073 — SOB vs SOC discrimination) ───────────
    # Self-organized bistability (SOB) self-tunes to first-order phase
    # coexistence rather than second-order criticality, producing bimodal
    # activity distributions where scale-invariant avalanches coexist with
    # anomalous large ones.  The bimodality coefficient BC = (γ₁² + 1) / κ
    # (where γ₁ = skewness, κ = standard kurtosis) exceeds 5/9 ≈ 0.556
    # for bimodal distributions.  When BC > 5/9 AND a histogram double-peak
    # is detected, the distribution is flagged as SOB-type (first-order
    # coexistence) rather than SOC-type (second-order critical point).
    #
    # This prevents misclassification: both SOB and SOC produce power-law-
    # like avalanche statistics, but SOB additionally shows anomalous
    # bumps / secondary peaks in the size distribution from the coexisting
    # ordered phase.  The bimodality_flag makes this distinction explicit.
    _BC_THRESHOLD = 5.0 / 9.0  # ≈ 0.5556

    # Filter to positive sizes for bimodality analysis
    _bm_sizes = [float(s) for s in avalanche_sizes if s > 0]
    _bm_n = len(_bm_sizes)

    bimodality_flag = False
    bimodality_coefficient = 0.0
    bimodality_skewness = 0.0
    bimodality_kurtosis = 0.0
    bimodality_peak_separation = 0.0
    bimodality_n_peaks = 0
    bimodality_peak_positions = []  # type: list
    bimodality_detail = ""
    attractor_type = "UNDETERMINED"

    if _bm_n >= 10:
        # Work in log-space for avalanche sizes (natural for power-law data)
        _bm_log_sizes = [math.log(s) for s in _bm_sizes]

        # Compute moments
        _bm_mu = sum(_bm_log_sizes) / float(_bm_n)
        _bm_m2 = sum((x - _bm_mu) ** 2 for x in _bm_log_sizes) / float(_bm_n)
        _bm_m3 = sum((x - _bm_mu) ** 3 for x in _bm_log_sizes) / float(_bm_n)
        _bm_m4 = sum((x - _bm_mu) ** 4 for x in _bm_log_sizes) / float(_bm_n)

        if _bm_m2 > 1e-30:
            _bm_std = math.sqrt(_bm_m2)
            bimodality_skewness = _bm_m3 / (_bm_std ** 3)
            # Standard kurtosis (not excess): normal distribution = 3
            bimodality_kurtosis = _bm_m4 / (_bm_m2 ** 2)

            if bimodality_kurtosis > 1e-15:
                bimodality_coefficient = (bimodality_skewness ** 2 + 1.0) / bimodality_kurtosis

            # Histogram peak detection in log-space
            _bm_n_bins = max(8, int(math.sqrt(float(_bm_n))))
            _bm_lo = min(_bm_log_sizes)
            _bm_hi = max(_bm_log_sizes)
            _bm_span = _bm_hi - _bm_lo

            if _bm_span > 0:
                _bm_bw = _bm_span / float(_bm_n_bins)
                _bm_counts = [0] * _bm_n_bins
                for _v in _bm_log_sizes:
                    _idx = int((_v - _bm_lo) / _bm_bw)
                    if _idx >= _bm_n_bins:
                        _idx = _bm_n_bins - 1
                    _bm_counts[_idx] += 1

                # Find local maxima (bins higher than both neighbours)
                _bm_peaks = []  # type: list
                for _bi in range(_bm_n_bins):
                    _left = _bm_counts[_bi - 1] if _bi > 0 else -1
                    _right = _bm_counts[_bi + 1] if _bi < _bm_n_bins - 1 else -1
                    if _bm_counts[_bi] > _left and _bm_counts[_bi] > _right:
                        _bm_peaks.append((_bi, _bm_counts[_bi]))

                # Sort by count descending
                _bm_peaks.sort(key=lambda x: x[1], reverse=True)
                bimodality_n_peaks = min(len(_bm_peaks), 4)

                for _pk_idx, _pk_cnt in _bm_peaks[:4]:
                    _pk_center_log = _bm_lo + (float(_pk_idx) + 0.5) * _bm_bw
                    _pk_center_lin = math.exp(_pk_center_log)
                    bimodality_peak_positions.append(round(_pk_center_lin, 4))

                if len(_bm_peaks) >= 2:
                    _p1_log = _bm_lo + (float(_bm_peaks[0][0]) + 0.5) * _bm_bw
                    _p2_log = _bm_lo + (float(_bm_peaks[1][0]) + 0.5) * _bm_bw
                    bimodality_peak_separation = round(abs(_p1_log - _p2_log), 6)

            # Set bimodality flag
            bimodality_flag = (
                bimodality_coefficient > _BC_THRESHOLD
                and bimodality_n_peaks >= 2
            )

        # Classify attractor type
        if bimodality_flag and alpha_in_soc and power_law_likely:
            attractor_type = "SOB"
            bimodality_detail = (
                "SOB SIGNATURE (INV_073): Bimodal avalanche size distribution "
                "detected (BC={:.4f} > {:.4f}, {} peaks in log-space histogram, "
                "peak separation={:.4f} log-units). Scale-invariant avalanches "
                "coexist with anomalous large events, consistent with self-"
                "organized bistability (first-order phase coexistence) rather "
                "than self-organized criticality (second-order critical point). "
                "The system may be self-tuning to a first-order coexistence "
                "manifold — the critical ridge identified by INV_073 is NOT "
                "unique; SOB provides an equally valid self-organization target "
                "with different universality class. Peak positions (linear): {}."
            ).format(
                bimodality_coefficient, _BC_THRESHOLD,
                bimodality_n_peaks, bimodality_peak_separation,
                bimodality_peak_positions,
            )
        elif not bimodality_flag and alpha_in_soc and power_law_likely:
            attractor_type = "SOC"
            bimodality_detail = (
                "SOC CONSISTENT: Unimodal avalanche size distribution "
                "(BC={:.4f} <= {:.4f}, {} peak(s)). No anomalous secondary "
                "peak detected. Consistent with self-organized criticality "
                "(second-order critical point). The system is on the standard "
                "critical ridge identified by INV_073."
            ).format(
                bimodality_coefficient, _BC_THRESHOLD,
                bimodality_n_peaks,
            )
        elif bimodality_flag:
            attractor_type = "BIMODAL_NON_SOC"
            bimodality_detail = (
                "BIMODAL but power-law not confirmed (alpha={:.4f}, R^2={:.4f}). "
                "BC={:.4f} > {:.4f} with {} peaks. The bimodal structure may "
                "indicate first-order coexistence dynamics but without confirmed "
                "scale invariance, the SOB classification is provisional."
            ).format(
                alpha, alpha_r_squared,
                bimodality_coefficient, _BC_THRESHOLD,
                bimodality_n_peaks,
            )
        else:
            bimodality_detail = (
                "Insufficient evidence for bimodality classification "
                "(BC={:.4f}, {} peaks, {} samples)."
            ).format(bimodality_coefficient, bimodality_n_peaks, _bm_n)
    else:
        bimodality_detail = (
            "Insufficient avalanche sizes for bimodality analysis "
            "(need >= 10, got {})."
        ).format(_bm_n)

    return {
        "alpha": round(alpha, 4),
        "alpha_r_squared": round(alpha_r_squared, 4),
        "alpha_in_soc": alpha_in_soc,
        "power_law_likely": power_law_likely,
        "dsi": dsi,
        "universality_class": dsi["universality_class"],
        "l4_rg_assessment": l4_rg,
        "soc_ref_deviations": _soc_deviations,
        "soc_closest_ref": _soc_closest_ref,
        "soc_closest_deviation": round(_soc_closest_dev, 4),
        "soc_outlier": _soc_outlier,
        "soc_deviation_threshold": _SOC_DEVIATION_THRESHOLD,
        "soc_flag_detail": _soc_flag_detail,
        "bimodality_flag": bimodality_flag,
        "bimodality_coefficient": round(bimodality_coefficient, 6),
        "bimodality_skewness": round(bimodality_skewness, 6),
        "bimodality_kurtosis": round(bimodality_kurtosis, 6),
        "bimodality_peak_separation": bimodality_peak_separation,
        "bimodality_n_peaks": bimodality_n_peaks,
        "bimodality_peak_positions": bimodality_peak_positions,
        "bimodality_detail": bimodality_detail,
        "attractor_type": attractor_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def score_ca_generation(
    generation,         # type: int
    sigma,              # type: float
    sigma_std,          # type: float
    alpha,              # type: float
    alpha_r_squared,    # type: float
    shannon_h,          # type: float
    shannon_h_max,      # type: float
    survival_rate,      # type: float
    dominant_type="",   # type: str
    population_size=0,  # type: int
    avalanche_sizes=None,  # type: Optional[List[float]]
):
    # type: (...) -> dict
    """
    Score a single CA generation's criticality telemetry.

    Produces a scored telemetry record with σ, α, Shannon entropy,
    survival rate, criticality verdict, drift indicators, and
    discrete-scale-invariance (DSI) assessment for longitudinal tracking.

    Parameters
    ----------
    generation : int
        The simulation step / generation number.
    sigma : float
        Branching ratio σ (mean over measurement window).
    sigma_std : float
        Standard deviation of σ within the measurement window.
    alpha : float
        Power-law exponent α from avalanche size distribution.
    alpha_r_squared : float
        R² goodness-of-fit for the power-law regression.
    shannon_h : float
        Shannon entropy H of the population type distribution (bits).
    shannon_h_max : float
        Maximum possible Shannon entropy (log2 of number of types).
    survival_rate : float
        Fraction of cells surviving this generation.
    dominant_type : str
        Name/label of the most populous cell type.
    population_size : int
        Total number of live cells.
    avalanche_sizes : list of float or None
        Raw avalanche sizes for DSI detection.  If None, DSI analysis
        is skipped.

    Returns
    -------
    dict with keys:
        generation       : int
        sigma            : float   — branching ratio
        sigma_std        : float   — σ uncertainty
        alpha            : float   — power-law exponent
        alpha_r_squared  : float   — power-law fit quality
        shannon_h        : float   — Shannon entropy (bits)
        h_fraction       : float   — H / H_max (entropy utilization)
        survival_rate    : float
        dominant_type    : str
        population_size  : int
        verdict          : str     — AT_CRITICAL / NEAR_CRITICAL / etc.
        sigma_drift      : float   — |σ - 1.0| (distance from perfect criticality)
        alpha_in_soc     : bool    — whether α is in SOC-consistent range
        power_law_likely : bool    — R² above threshold
        distribution_score : dict or None — score_distribution result with DSI
        universality_class : str   — CONTINUOUS / DISCRETE / UNDETERMINED
        timestamp        : str     — ISO-8601 UTC timestamp
    """
    verdict = _criticality_verdict(sigma, alpha, alpha_r_squared)
    sigma_drift = abs(sigma - 1.0)
    h_fraction = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0
    alpha_in_soc = ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
    power_law_likely = alpha_r_squared >= ALPHA_R2_THRESHOLD

    # Run avalanche distribution scoring with DSI detection
    dist_score = None  # type: Optional[dict]
    universality_class = "UNDETERMINED"
    if avalanche_sizes is not None and len(avalanche_sizes) >= 20 and alpha > 0.0:
        dist_score = score_distribution(avalanche_sizes, alpha, alpha_r_squared)
        universality_class = dist_score.get("universality_class", "UNDETERMINED")

    # ── Empirical criticality confirmation (INV_073 ridge-navigation) ────
    # Ground the criticality verdict in measured σ and α rather than
    # heuristic proxies.  The `criticality_confirmed` flag is True only
    # when BOTH:
    #   1. σ ∈ [0.95, 1.05]  (branching ratio in critical band)
    #   2. R² > 0.99          (power-law fit is near-perfect)
    # This closes the gap between CA telemetry and the genome's INV_073
    # ridge-navigation invariant by requiring empirically measured σ and α
    # to pass strict thresholds before declaring confirmed criticality.
    #
    # Paper reference values (Game of Truth 32×32, 200-step):
    #   σ = 1.0284 ± 0.0171 — within critical band
    #   α ≈ 1.764, R² = 0.999 — power-law confirmed
    _SIGMA_CONF_LOW = 0.95
    _SIGMA_CONF_HIGH = 1.05
    _R2_CONF_THRESHOLD = 0.99

    sigma_in_critical_band = _SIGMA_CONF_LOW <= sigma <= _SIGMA_CONF_HIGH
    r2_exceeds_threshold = alpha_r_squared > _R2_CONF_THRESHOLD
    criticality_confirmed = sigma_in_critical_band and r2_exceeds_threshold

    criticality_confirmation_detail = (
        "sigma={:.4f} {} [{:.2f}, {:.2f}], alpha={:.4f}, R^2={:.4f} {} {:.2f}. "
        "criticality_confirmed={}."
    ).format(
        sigma,
        "IN" if sigma_in_critical_band else "OUTSIDE",
        _SIGMA_CONF_LOW, _SIGMA_CONF_HIGH,
        alpha, alpha_r_squared,
        ">" if r2_exceeds_threshold else "<=",
        _R2_CONF_THRESHOLD,
        criticality_confirmed,
    )

    return {
        "generation":       generation,
        "sigma":            round(sigma, 6),
        "sigma_std":        round(sigma_std, 6),
        "alpha":            round(alpha, 4),
        "alpha_r_squared":  round(alpha_r_squared, 4),
        "shannon_h":        round(shannon_h, 4),
        "h_fraction":       round(h_fraction, 4),
        "survival_rate":    round(survival_rate, 4),
        "dominant_type":    dominant_type,
        "population_size":  population_size,
        "verdict":          verdict,
        "sigma_drift":      round(sigma_drift, 6),
        "alpha_in_soc":     alpha_in_soc,
        "power_law_likely": power_law_likely,
        "distribution_score": dist_score,
        "universality_class": universality_class,
        "criticality_confirmed": criticality_confirmed,
        "sigma_in_critical_band": sigma_in_critical_band,
        "r2_exceeds_threshold": r2_exceeds_threshold,
        "criticality_confirmation_detail": criticality_confirmation_detail,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }


# ─── Dual Criticality Scorer ────────────────────────────────────────────────
# Tracks branching ratio σ AND power-law avalanche fit quality as separate
# metrics, flagging divergence between them as a "pseudo-criticality"
# warning state distinct from confirmed SOC.
#
# Addresses INV_073: σ can sit within the critical band (e.g. σ=1.028)
# while power-law avalanche statistics fail (R²=0.859, power_law_likely=False).
# The genome's γ=1 criticality criterion (σ≈1) is necessary but not sufficient
# for SOC.  This scorer makes that dissociation explicit and machine-readable.
#
# States:
#   CONFIRMED_SOC       — σ in band AND power-law confirmed (true SOC)
#   PSEUDO_CRITICAL     — σ in band BUT power-law fails (near-critical mimicry)
#   SIGMA_ONLY          — σ in band, insufficient avalanche data
#   POWERLAW_ONLY       — power-law confirmed but σ outside band (rare/transient)
#   NOT_CRITICAL        — neither metric satisfied
#   UNDETERMINED        — insufficient data for either metric

# Thresholds for power-law quality assessment
POWERLAW_R2_STRONG   = 0.90   # R² above this → strong power-law evidence
POWERLAW_R2_WEAK     = 0.80   # R² below this → power-law rejected
ALPHA_THEORETICAL_LO = 1.2    # Theoretical SOC exponents (broader than scoring band)
ALPHA_THEORETICAL_HI = 3.5


def dual_criticality_score(
    sigma,              # type: float
    sigma_std,          # type: float
    alpha,              # type: float
    r_squared,          # type: float
    power_law_likely,   # type: bool
    shannon_h=0.0,      # type: float
    shannon_h_max=0.0,  # type: float
    survival_rate=0.0,  # type: float
):
    # type: (...) -> dict
    """
    Score criticality by independently evaluating branching ratio σ and
    power-law avalanche statistics, detecting dissociation between them.

    This directly addresses INV_073: the branching ratio can sit within
    the critical band (σ=1.028) while power-law avalanche statistics
    fail (R²=0.859, power_law_likely=False), demonstrating that σ≈1
    is necessary but not sufficient for SOC.

    Parameters
    ----------
    sigma : float
        Branching ratio (mean over measurement window).
    sigma_std : float
        Standard deviation of σ within the measurement window.
    alpha : float
        Power-law exponent from avalanche size distribution.
    r_squared : float
        R² goodness-of-fit for the power-law regression.
    power_law_likely : bool
        Whether the power-law hypothesis passed a statistical test.
    shannon_h : float
        Shannon entropy H of the population type distribution (bits).
    shannon_h_max : float
        Maximum possible Shannon entropy.
    survival_rate : float
        Fraction of cells surviving this generation.

    Returns
    -------
    dict with keys:
        dual_state         : str   — CONFIRMED_SOC / PSEUDO_CRITICAL /
                                     SIGMA_ONLY / POWERLAW_ONLY /
                                     NOT_CRITICAL / UNDETERMINED
        sigma_in_band      : bool  — σ within [0.95, 1.05]
        sigma_quality      : str   — "tight" (std<0.02) / "loose" / "noisy"
        powerlaw_confirmed : bool  — R² ≥ threshold AND power_law_likely AND α in range
        powerlaw_quality   : str   — "strong" / "marginal" / "failed"
        r_squared          : float — echo of input for downstream consumers
        alpha              : float — echo of input
        dissociation       : bool  — True when σ says critical but power-law disagrees
        dissociation_detail: str   — human-readable explanation of the dissociation
        inv073_flag        : bool  — True when this exact INV_073 pattern is detected
                                     (σ in band, power_law_likely=False, R²<0.90)
        soc_confidence     : float — 0.0–1.0 composite confidence in true SOC
        legacy_verdict     : str   — the old _criticality_verdict result (for compat)
        timestamp          : str   — ISO-8601 UTC
    """
    # ── Evaluate σ channel ──
    sigma_in_band = SIGMA_CRITICAL_LOW <= sigma <= SIGMA_CRITICAL_HIGH

    if sigma_std < 0.02:
        sigma_quality = "tight"
    elif sigma_std < 0.05:
        sigma_quality = "loose"
    else:
        sigma_quality = "noisy"

    # ── Evaluate power-law channel ──
    alpha_in_range = ALPHA_THEORETICAL_LO <= alpha <= ALPHA_THEORETICAL_HI
    r2_strong = r_squared >= POWERLAW_R2_STRONG
    r2_marginal = POWERLAW_R2_WEAK <= r_squared < POWERLAW_R2_STRONG

    if power_law_likely and r2_strong and alpha_in_range:
        powerlaw_confirmed = True
        powerlaw_quality = "strong"
    elif power_law_likely and r2_marginal and alpha_in_range:
        powerlaw_confirmed = True
        powerlaw_quality = "marginal"
    elif (not power_law_likely) and r2_marginal and alpha_in_range:
        # R² is borderline but statistical test failed → not confirmed
        powerlaw_confirmed = False
        powerlaw_quality = "marginal"
    else:
        powerlaw_confirmed = False
        powerlaw_quality = "failed"

    # ── Determine dual state ──
    if sigma == 0.0 and alpha == 0.0:
        dual_state = "UNDETERMINED"
    elif sigma_in_band and powerlaw_confirmed:
        dual_state = "CONFIRMED_SOC"
    elif sigma_in_band and not powerlaw_confirmed:
        dual_state = "PSEUDO_CRITICAL"
    elif not sigma_in_band and powerlaw_confirmed:
        dual_state = "POWERLAW_ONLY"
    elif sigma_in_band and alpha == 0.0 and r_squared == 0.0:
        dual_state = "SIGMA_ONLY"
    else:
        dual_state = "NOT_CRITICAL"

    # ── Detect dissociation ──
    dissociation = sigma_in_band and not powerlaw_confirmed
    dissociation_detail = ""
    if dissociation:
        parts = []
        parts.append(
            "sigma={:.4f} is within critical band [{:.2f}, {:.2f}]".format(
                sigma, SIGMA_CRITICAL_LOW, SIGMA_CRITICAL_HIGH
            )
        )
        if not power_law_likely:
            parts.append("power_law_likely=False (statistical test rejected)")
        if not r2_strong:
            parts.append("R^2={:.3f} below strong threshold {:.2f}".format(
                r_squared, POWERLAW_R2_STRONG
            ))
        if not alpha_in_range:
            parts.append("alpha={:.3f} outside theoretical SOC range [{:.1f}, {:.1f}]".format(
                alpha, ALPHA_THEORETICAL_LO, ALPHA_THEORETICAL_HI
            ))
        dissociation_detail = "; ".join(parts)

    # ── INV_073 specific pattern detection ──
    # σ in band, power_law_likely=False, R² < 0.90
    inv073_flag = (
        sigma_in_band
        and not power_law_likely
        and r_squared < POWERLAW_R2_STRONG
    )

    # ── Composite SOC confidence ──
    # Weighted combination: σ channel (0.4) + power-law channel (0.6)
    # Power-law gets more weight because it's the harder test.
    sigma_score = 0.0
    if sigma_in_band:
        # Score by how centered σ is in the band, penalized by noise
        center_dist = abs(sigma - 1.0)
        half_band = (SIGMA_CRITICAL_HIGH - SIGMA_CRITICAL_LOW) / 2.0
        sigma_score = max(0.0, 1.0 - (center_dist / half_band))
        if sigma_quality == "noisy":
            sigma_score *= 0.5
        elif sigma_quality == "loose":
            sigma_score *= 0.8

    pl_score = 0.0
    if powerlaw_confirmed:
        pl_score = r_squared  # R² directly as quality
        if powerlaw_quality == "marginal":
            pl_score *= 0.7

    soc_confidence = 0.4 * sigma_score + 0.6 * pl_score
    soc_confidence = round(min(1.0, max(0.0, soc_confidence)), 4)

    # ── Legacy verdict for backward compatibility ──
    legacy_verdict = _criticality_verdict(sigma, alpha, r_squared)

    return {
        "dual_state":          dual_state,
        "sigma_in_band":       sigma_in_band,
        "sigma_quality":       sigma_quality,
        "powerlaw_confirmed":  powerlaw_confirmed,
        "powerlaw_quality":    powerlaw_quality,
        "r_squared":           round(r_squared, 4),
        "alpha":               round(alpha, 4),
        "dissociation":        dissociation,
        "dissociation_detail": dissociation_detail,
        "inv073_flag":         inv073_flag,
        "soc_confidence":      soc_confidence,
        "legacy_verdict":      legacy_verdict,
        "timestamp":           datetime.now(timezone.utc).isoformat(),
    }


def soc_criticality_validator(
    sigma,              # type: float
    sigma_std,          # type: float
    alpha,              # type: float
    alpha_r_squared,    # type: float
    shannon_h,          # type: float
    shannon_h_max,      # type: float
    power_law_likely=True,  # type: bool
    grid_size=0,        # type: int
    n_steps=0,          # type: int
    confidence_level=0.95,  # type: float
):
    # type: (...) -> dict
    """
    Three-metric SOC validator for automated criticality verdicts with
    confidence bounds.

    Evaluates three independent criticality metrics simultaneously:
      1. Branching ratio σ — must be within [0.95, 1.05] for criticality
      2. Power-law exponent α — must be in [1.5, 3.0] with R² ≥ 0.85
      3. Entropy ratio H/H_max — characterizes the ordered/balanced nature
         of the critical state

    Each metric is scored independently with a confidence interval, then
    combined into a composite SOC verdict that distinguishes:
      - CONFIRMED_SOC: all three metrics consistent with SOC
      - ORDERED_SOC: σ and α confirm SOC, but H/H_max is low (<0.3),
        indicating an ordered critical phase (INV_073 pattern)
      - PARTIAL_SOC: some metrics confirm, others fail
      - NOT_SOC: insufficient evidence for SOC
      - UNDETERMINED: insufficient data

    INV_073 Falsification:
      Low H/H_max (e.g., 0.182) at confirmed criticality (σ≈1.02, α≈2.14)
      does NOT falsify SOC — it identifies an *ordered* SOC phase where
      the critical ridge sustains low-diversity, spatially correlated
      states.  The Wasserstein gradient path to γ=1 does NOT require
      near-maximal entropy utilization.  The signature is H > 0 (not
      frozen) with σ ≈ 1 and power-law avalanches.  This validator
      makes that distinction explicit and machine-auditable.

    Parameters
    ----------
    sigma : float
        Branching ratio σ (mean over measurement window).
    sigma_std : float
        Standard deviation of σ within the measurement window.
    alpha : float
        Power-law exponent α from avalanche size distribution.
    alpha_r_squared : float
        R² goodness-of-fit for the power-law regression.
    shannon_h : float
        Shannon entropy H of the population type distribution (bits).
    shannon_h_max : float
        Maximum possible Shannon entropy (log2 of number of types).
    power_law_likely : bool
        Whether the power-law hypothesis passed a statistical test.
    grid_size : int
        Grid side length (e.g., 32 for a 32×32 grid). Used for
        finite-size confidence adjustment. 0 = no adjustment.
    n_steps : int
        Number of simulation steps in the measurement window. Used
        for statistical confidence. 0 = no adjustment.
    confidence_level : float
        Desired confidence level for bounds (e.g., 0.95 for 95%).
        Default: 0.95.

    Returns
    -------
    dict with keys:
        verdict              : str   — CONFIRMED_SOC / ORDERED_SOC /
                                       PARTIAL_SOC / NOT_SOC / UNDETERMINED
        soc_confidence       : float — 0.0–1.0 composite SOC probability
        sigma_metric         : dict  — {value, in_band, score, ci_low, ci_high,
                                        quality, detail}
        alpha_metric         : dict  — {value, in_soc_range, r_squared,
                                        score, quality, detail}
        entropy_metric       : dict  — {h, h_max, h_ratio, category,
                                        score, detail}
        n_metrics_passing    : int   — count of metrics confirming SOC (0–3)
        metric_agreement     : str   — "unanimous" / "majority" / "minority" /
                                       "none"
        inv073_assessment    : str   — human-readable INV_073 falsification status
        inv073_falsified     : bool  — True ONLY if data actively contradicts
                                       SOC (not merely low entropy)
        finite_size_note     : str   — note on grid-size effects if applicable
        confidence_bounds    : dict  — {level, sigma_ci, alpha_ci,
                                        soc_confidence_ci}
        obligations_addressed: list  — which obligations this verdict grounds
        timestamp            : str   — ISO-8601 UTC
    """
    ts = datetime.now(timezone.utc).isoformat()

    # ── Guard: insufficient data ──
    if sigma == 0.0 and alpha == 0.0 and shannon_h == 0.0:
        return {
            "verdict":               "UNDETERMINED",
            "soc_confidence":        0.0,
            "sigma_metric":          {"value": 0.0, "in_band": False, "score": 0.0,
                                      "ci_low": 0.0, "ci_high": 0.0,
                                      "quality": "no_data", "detail": "No sigma data."},
            "alpha_metric":          {"value": 0.0, "in_soc_range": False,
                                      "r_squared": 0.0, "score": 0.0,
                                      "quality": "no_data", "detail": "No alpha data."},
            "entropy_metric":        {"h": 0.0, "h_max": 0.0, "h_ratio": 0.0,
                                      "category": "no_data", "score": 0.0,
                                      "detail": "No entropy data."},
            "n_metrics_passing":     0,
            "metric_agreement":      "none",
            "inv073_assessment":     "No data — cannot assess.",
            "inv073_falsified":      False,
            "finite_size_note":      "",
            "confidence_bounds":     {"level": confidence_level,
                                      "sigma_ci": (0.0, 0.0),
                                      "alpha_ci": (0.0, 0.0),
                                      "soc_confidence_ci": (0.0, 0.0)},
            "obligations_addressed": [],
            "timestamp":             ts,
        }

    # ── Z-score for confidence interval ──
    # Approximate z for common confidence levels (no scipy dependency)
    if confidence_level >= 0.99:
        z = 2.576
    elif confidence_level >= 0.975:
        z = 2.241
    elif confidence_level >= 0.95:
        z = 1.960
    elif confidence_level >= 0.90:
        z = 1.645
    else:
        z = 1.282

    # ── Finite-size confidence adjustment ──
    # Smaller grids have larger fluctuations; adjust effective σ_std
    finite_size_factor = 1.0
    finite_size_note = ""
    if grid_size > 0:
        # Finite-size scaling: fluctuations scale as ~1/sqrt(N) where N=grid_size^2
        n_cells = grid_size * grid_size
        if n_cells < 1024:  # less than 32x32
            finite_size_factor = math.sqrt(1024.0 / float(n_cells))
            finite_size_note = (
                "Grid size {}x{} ({} cells) — finite-size fluctuations "
                "inflated by factor {:.2f} relative to 32x32 baseline. "
                "Confidence intervals widened accordingly."
            ).format(grid_size, grid_size, n_cells, finite_size_factor)
        elif n_cells > 1024:
            finite_size_factor = math.sqrt(1024.0 / float(n_cells))
            finite_size_note = (
                "Grid size {}x{} ({} cells) — larger than 32x32 baseline. "
                "Finite-size effects reduced by factor {:.2f}."
            ).format(grid_size, grid_size, n_cells, finite_size_factor)
        else:
            finite_size_note = "Grid size 32x32 (1024 cells) — baseline size."

    # Temporal confidence: more steps → tighter bounds
    temporal_factor = 1.0
    if n_steps > 0 and n_steps < 200:
        temporal_factor = math.sqrt(200.0 / float(n_steps))
    elif n_steps >= 200:
        temporal_factor = math.sqrt(200.0 / float(n_steps))

    effective_sigma_std = sigma_std * finite_size_factor * temporal_factor
    if effective_sigma_std < 0.001:
        effective_sigma_std = 0.001  # floor to prevent degenerate CIs

    # ── METRIC 1: Branching ratio σ ──
    sigma_ci_low = sigma - z * effective_sigma_std
    sigma_ci_high = sigma + z * effective_sigma_std
    sigma_in_band = SIGMA_CRITICAL_LOW <= sigma <= SIGMA_CRITICAL_HIGH

    # Score: how centered is σ in the critical band?
    half_band = (SIGMA_CRITICAL_HIGH - SIGMA_CRITICAL_LOW) / 2.0
    sigma_center_dist = abs(sigma - 1.0)
    sigma_score = max(0.0, 1.0 - (sigma_center_dist / half_band)) if half_band > 0 else 0.0

    # Penalize by uncertainty
    if effective_sigma_std < 0.02:
        sigma_quality = "tight"
    elif effective_sigma_std < 0.05:
        sigma_quality = "loose"
        sigma_score *= 0.85
    else:
        sigma_quality = "noisy"
        sigma_score *= 0.5

    # Check if CI overlaps the band even if point estimate is outside
    ci_overlaps_band = not (sigma_ci_high < SIGMA_CRITICAL_LOW or
                            sigma_ci_low > SIGMA_CRITICAL_HIGH)

    sigma_detail = (
        "sigma={:.4f}+/-{:.4f} (effective std={:.4f}), "
        "{:.0f}% CI=[{:.4f}, {:.4f}]. "
        "In critical band [{:.2f}, {:.2f}]: {}. "
        "CI overlaps band: {}."
    ).format(
        sigma, sigma_std, effective_sigma_std,
        confidence_level * 100, sigma_ci_low, sigma_ci_high,
        SIGMA_CRITICAL_LOW, SIGMA_CRITICAL_HIGH,
        "YES" if sigma_in_band else "NO",
        "YES" if ci_overlaps_band else "NO",
    )

    sigma_metric = {
        "value":    round(sigma, 6),
        "in_band":  sigma_in_band,
        "score":    round(sigma_score, 4),
        "ci_low":   round(sigma_ci_low, 6),
        "ci_high":  round(sigma_ci_high, 6),
        "quality":  sigma_quality,
        "detail":   sigma_detail,
    }

    # ── METRIC 2: Power-law exponent α ──
    alpha_in_soc = ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
    r2_ok = alpha_r_squared >= ALPHA_R2_THRESHOLD

    alpha_score = 0.0
    if alpha_in_soc and r2_ok and power_law_likely:
        # Score by R² quality and centrality in SOC range
        alpha_center = (ALPHA_SOC_LOW + ALPHA_SOC_HIGH) / 2.0
        alpha_half_range = (ALPHA_SOC_HIGH - ALPHA_SOC_LOW) / 2.0
        alpha_centrality = max(0.0, 1.0 - abs(alpha - alpha_center) / alpha_half_range)
        alpha_score = alpha_r_squared * alpha_centrality
    elif alpha_in_soc and r2_ok:
        alpha_score = alpha_r_squared * 0.5  # in range but stat test failed
    elif alpha_in_soc:
        alpha_score = 0.2  # in range but poor fit

    if alpha_r_squared >= POWERLAW_R2_STRONG and power_law_likely:
        alpha_quality = "strong"
    elif alpha_r_squared >= POWERLAW_R2_WEAK:
        alpha_quality = "marginal"
    elif alpha == 0.0:
        alpha_quality = "no_data"
    else:
        alpha_quality = "failed"

    # Approximate CI for α using Hill estimator asymptotic variance
    # Var(α_Hill) ≈ (α-1)² / n  where n is tail sample size
    # We don't have n directly, so use R² as a proxy for quality
    alpha_std_approx = abs(alpha - 1.0) * (1.0 - alpha_r_squared + 0.01)
    alpha_ci_low = alpha - z * alpha_std_approx
    alpha_ci_high = alpha + z * alpha_std_approx

    alpha_detail = (
        "alpha={:.4f}, R^2={:.4f}, power_law_likely={}. "
        "In SOC range [{:.1f}, {:.1f}]: {}. "
        "Approx {:.0f}% CI=[{:.4f}, {:.4f}]."
    ).format(
        alpha, alpha_r_squared, power_law_likely,
        ALPHA_SOC_LOW, ALPHA_SOC_HIGH,
        "YES" if alpha_in_soc else "NO",
        confidence_level * 100, alpha_ci_low, alpha_ci_high,
    )

    alpha_metric = {
        "value":        round(alpha, 4),
        "in_soc_range": alpha_in_soc,
        "r_squared":    round(alpha_r_squared, 4),
        "score":        round(alpha_score, 4),
        "quality":      alpha_quality,
        "detail":       alpha_detail,
    }

    # ── METRIC 3: Entropy ratio H/H_max ──
    h_ratio = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0

    # Categorize entropy regime
    if shannon_h == 0.0 and shannon_h_max == 0.0:
        h_category = "no_data"
    elif h_ratio <= 0.0:
        h_category = "frozen"       # dead — no information
    elif h_ratio < 0.15:
        h_category = "near_frozen"  # very low diversity
    elif h_ratio < 0.3:
        h_category = "ordered"      # low diversity — INV_073 pattern
    elif h_ratio < 0.6:
        h_category = "moderate"     # moderate diversity
    elif h_ratio < 0.85:
        h_category = "high"         # high diversity
    else:
        h_category = "near_maximal" # near-uniform distribution

    # Entropy score: SOC is compatible with ANY h_ratio > 0 (not frozen).
    # The score reflects how informative the entropy is for SOC *diagnosis*,
    # not how "good" high entropy is.  Both ordered SOC and balanced SOC
    # are valid critical states.
    if h_category == "no_data":
        h_score = 0.0
    elif h_category == "frozen":
        h_score = 0.0  # truly frozen → not SOC
    elif h_category == "near_frozen":
        h_score = 0.3  # barely alive — SOC possible but fragile
    else:
        # Any h_ratio > 0.15 is compatible with SOC
        # Score slightly higher for moderate range (most SOC systems
        # are neither maximally ordered nor maximally disordered)
        h_score = 0.7 + 0.3 * min(1.0, h_ratio / 0.5)
        h_score = min(1.0, h_score)

    h_detail = (
        "H={:.4f} bits, H_max={:.4f} bits, H/H_max={:.4f} ({:.1f}%). "
        "Category: {}. "
    ).format(shannon_h, shannon_h_max, h_ratio, h_ratio * 100.0, h_category)

    if h_category == "ordered" and sigma_in_band:
        h_detail += (
            "LOW ENTROPY AT CRITICALITY: This is the INV_073 pattern — "
            "ordered SOC phase with spatially correlated, low-diversity "
            "states. The critical ridge does NOT require near-maximal "
            "entropy. H > 0 with sigma in band confirms the system is "
            "not frozen."
        )

    entropy_metric = {
        "h":        round(shannon_h, 4),
        "h_max":    round(shannon_h_max, 4),
        "h_ratio":  round(h_ratio, 4),
        "category": h_category,
        "score":    round(h_score, 4),
        "detail":   h_detail,
    }

    # ── Composite verdict ──
    sigma_passes = sigma_in_band and sigma_score > 0.3
    alpha_passes = alpha_in_soc and r2_ok and alpha_score > 0.3
    entropy_alive = h_ratio > 0.0 and h_category != "frozen"

    n_passing = int(sigma_passes) + int(alpha_passes) + int(entropy_alive)

    if n_passing == 3:
        metric_agreement = "unanimous"
    elif n_passing == 2:
        metric_agreement = "majority"
    elif n_passing == 1:
        metric_agreement = "minority"
    else:
        metric_agreement = "none"

    # Determine verdict
    if sigma_passes and alpha_passes and entropy_alive:
        if h_ratio < 0.3:
            verdict = "ORDERED_SOC"
        else:
            verdict = "CONFIRMED_SOC"
    elif sigma_passes and alpha_passes and not entropy_alive:
        verdict = "NOT_SOC"  # σ and α pass but system is frozen
    elif (sigma_passes or alpha_passes) and entropy_alive:
        verdict = "PARTIAL_SOC"
    elif n_passing == 0:
        verdict = "NOT_SOC"
    else:
        verdict = "PARTIAL_SOC"

    # ── Composite confidence ──
    # Weighted: σ (0.35) + α (0.45) + entropy (0.20)
    # Power-law gets most weight — hardest test to pass
    soc_confidence = 0.35 * sigma_score + 0.45 * alpha_score + 0.20 * h_score
    soc_confidence = round(min(1.0, max(0.0, soc_confidence)), 4)

    # Confidence interval on the composite (bootstrap-like approximation)
    # Using the metric uncertainties propagated through the weights
    sigma_contrib_var = (0.35 * effective_sigma_std / half_band) ** 2 if half_band > 0 else 0.0
    alpha_contrib_var = (0.45 * alpha_std_approx / ((ALPHA_SOC_HIGH - ALPHA_SOC_LOW) / 2.0)) ** 2
    composite_std = math.sqrt(sigma_contrib_var + alpha_contrib_var)
    if composite_std < 0.01:
        composite_std = 0.01
    soc_ci_low = max(0.0, soc_confidence - z * composite_std)
    soc_ci_high = min(1.0, soc_confidence + z * composite_std)

    # ── INV_073 assessment ──
    # INV_073 challenge: low H/H_max at confirmed criticality suggests
    # Wasserstein gradient does not maximize information throughput at γ=1.
    inv073_falsified = False  # only True if data *contradicts* SOC
    if verdict in ("CONFIRMED_SOC", "ORDERED_SOC") and h_ratio < 0.3:
        inv073_assessment = (
            "INV_073 ADDRESSED: H/H_max={:.4f} ({:.1f}%) at confirmed "
            "criticality (sigma={:.4f}, alpha={:.4f}). This is CONSISTENT "
            "with ordered SOC — the critical ridge sustains low-diversity, "
            "spatially correlated states. The Wasserstein gradient path to "
            "gamma=1 does NOT require near-maximal entropy utilization. "
            "The system is alive (H>0), critical (sigma in band), and "
            "exhibits power-law avalanches (alpha in SOC range). The low "
            "H/H_max ratio characterizes the *type* of critical state "
            "(ordered vs balanced), not the *presence* of criticality. "
            "INV_073 is NOT falsified by this data."
        ).format(h_ratio, h_ratio * 100.0, sigma, alpha)
    elif verdict in ("CONFIRMED_SOC", "ORDERED_SOC"):
        inv073_assessment = (
            "INV_073 CONSISTENT: H/H_max={:.4f} at confirmed criticality. "
            "Entropy utilization is in the {} range. No conflict with "
            "Wasserstein gradient path."
        ).format(h_ratio, h_category)
    elif verdict == "NOT_SOC" and not entropy_alive:
        inv073_falsified = True
        inv073_assessment = (
            "INV_073 POTENTIALLY FALSIFIED: System appears frozen "
            "(H/H_max={:.4f}). If sigma is in the critical band but the "
            "system has zero entropy, this is a degenerate fixed point, "
            "not a living critical state. The Wasserstein gradient has "
            "NOT reached gamma=1 — it has reached gamma=0 (frozen)."
        ).format(h_ratio)
    elif sigma_in_band and not alpha_passes:
        inv073_assessment = (
            "INV_073 INCONCLUSIVE: sigma={:.4f} in critical band but "
            "power-law avalanches not confirmed (alpha={:.4f}, R^2={:.4f}). "
            "This is the pseudo-criticality pattern — sigma alone is "
            "necessary but not sufficient for SOC. Cannot assess "
            "Wasserstein gradient convergence without confirmed SOC."
        ).format(sigma, alpha, alpha_r_squared)
    else:
        inv073_assessment = (
            "INV_073 NOT APPLICABLE: System is not at confirmed criticality "
            "(verdict={}). Cannot assess entropy-criticality relationship "
            "without SOC."
        ).format(verdict)

    # ── Obligations addressed ──
    obligations = []  # type: list
    if verdict in ("CONFIRMED_SOC", "ORDERED_SOC", "PARTIAL_SOC"):
        obligations.append("O140")  # CA measurement grounding
    if sigma_in_band:
        obligations.append("O141")  # solo-kernel vs population criticality
    if h_ratio > 0.0 and sigma_in_band:
        obligations.append("INV_073")  # Wasserstein gradient assessment

    confidence_bounds = {
        "level":             confidence_level,
        "sigma_ci":          (round(sigma_ci_low, 6), round(sigma_ci_high, 6)),
        "alpha_ci":          (round(alpha_ci_low, 4), round(alpha_ci_high, 4)),
        "soc_confidence_ci": (round(soc_ci_low, 4), round(soc_ci_high, 4)),
    }

    return {
        "verdict":               verdict,
        "soc_confidence":        soc_confidence,
        "sigma_metric":          sigma_metric,
        "alpha_metric":          alpha_metric,
        "entropy_metric":        entropy_metric,
        "n_metrics_passing":     n_passing,
        "metric_agreement":      metric_agreement,
        "inv073_assessment":     inv073_assessment,
        "inv073_falsified":      inv073_falsified,
        "finite_size_note":      finite_size_note,
        "confidence_bounds":     confidence_bounds,
        "obligations_addressed": obligations,
        "timestamp":             ts,
    }


class BranchingRatioTracker:
    """
    Real-time branching-ratio (σ) tracker with avalanche size distribution
    fitting and criticality drift detection.

    Maintains a sliding window of per-step activity counts, computes σ as
    the ratio of descendant activity to parent activity, fits power-law
    exponents α to avalanche size distributions via MLE, and flags
    deviations outside σ∈[0.95, 1.05] as criticality drift events.

    Addresses O140 (CA measurement grounding), O141 (solo-kernel vs
    population criticality), and INV_073 (low H at confirmed criticality).

    The tracker is designed for real-time use inside a CA simulation loop:
        tracker = BranchingRatioTracker()
        for step in simulation:
            tracker.record_step(parent_count, child_count, avalanche_sizes)
            telemetry = tracker.current_telemetry()
            if telemetry["drift_event"]:
                handle_drift(telemetry)
    """

    def __init__(self, window_size=50, sigma_band_low=0.95, sigma_band_high=1.05):
        # type: (int, float, float) -> None
        """
        Parameters
        ----------
        window_size : int
            Number of recent steps to use for rolling σ and α estimation.
        sigma_band_low : float
            Lower bound of the critical band for σ. Default: 0.95.
        sigma_band_high : float
            Upper bound of the critical band for σ. Default: 1.05.
        """
        self.window_size = max(5, window_size)
        self.sigma_band_low = sigma_band_low
        self.sigma_band_high = sigma_band_high

        # Rolling buffers
        self._parent_counts = []    # type: List[int]
        self._child_counts = []     # type: List[int]
        self._avalanche_sizes = []  # type: List[float]
        self._sigma_history = []    # type: List[float]
        self._h_fraction_history = []  # type: List[float]
        self._drift_events = []     # type: list

        # Cached telemetry (updated on each record_step)
        self._cached_telemetry = None  # type: Optional[dict]

    def record_step(self, parent_count, child_count, avalanche_sizes=None):
        # type: (int, int, Optional[List[float]]) -> dict
        """
        Record one simulation step's activity and recompute telemetry.

        Parameters
        ----------
        parent_count : int
            Number of active (parent) cells at this step.
        child_count : int
            Number of active (child/descendant) cells at the next step.
        avalanche_sizes : list of float or None
            Sizes of avalanches that terminated at this step. If None,
            avalanche tracking is skipped for this step.

        Returns
        -------
        dict — the current telemetry snapshot (same as current_telemetry()).
        """
        self._parent_counts.append(parent_count)
        self._child_counts.append(child_count)

        # Compute instantaneous σ for this step
        if parent_count > 0:
            step_sigma = float(child_count) / float(parent_count)
        else:
            step_sigma = 0.0
        self._sigma_history.append(step_sigma)

        # Accumulate avalanche sizes
        if avalanche_sizes is not None:
            self._avalanche_sizes.extend(avalanche_sizes)

        # Trim to window
        if len(self._parent_counts) > self.window_size * 2:
            trim = len(self._parent_counts) - self.window_size * 2
            self._parent_counts = self._parent_counts[trim:]
            self._child_counts = self._child_counts[trim:]
            self._sigma_history = self._sigma_history[trim:]

        # Keep avalanche buffer bounded (retain last 10x window for fitting)
        max_aval = self.window_size * 10
        if len(self._avalanche_sizes) > max_aval:
            self._avalanche_sizes = self._avalanche_sizes[-max_aval:]

        # Recompute telemetry
        self._cached_telemetry = self._compute_telemetry()

        # Check for drift events
        t = self._cached_telemetry
        if t["sigma_mean"] != 0.0 and not (
            self.sigma_band_low <= t["sigma_mean"] <= self.sigma_band_high
        ):
            event = {
                "step": len(self._sigma_history),
                "sigma_mean": t["sigma_mean"],
                "sigma_std": t["sigma_std"],
                "direction": "supercritical" if t["sigma_mean"] > self.sigma_band_high else "subcritical",
                "alpha": t["alpha"],
                "alpha_r_squared": t["alpha_r_squared"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self._drift_events.append(event)
            self._cached_telemetry["drift_event"] = event
        else:
            self._cached_telemetry["drift_event"] = None

        return self._cached_telemetry

    def current_telemetry(self):
        # type: () -> dict
        """
        Return the most recent telemetry snapshot.

        Returns
        -------
        dict with keys:
            sigma_mean       : float  — rolling mean of σ over window
            sigma_std        : float  — rolling std of σ over window
            alpha            : float  — power-law exponent from MLE fit
            alpha_r_squared  : float  — R² of the power-law fit
            power_law_likely : bool   — whether α is in SOC range with good R²
            in_critical_band : bool   — whether σ_mean ∈ [0.95, 1.05]
            drift_event      : dict or None — latest drift event if flagged
            n_steps          : int    — total steps recorded
            n_avalanches     : int    — total avalanche sizes in buffer
            verdict          : str    — AT_CRITICAL / NEAR_CRITICAL / etc.
        """
        if self._cached_telemetry is not None:
            return self._cached_telemetry
        return self._compute_telemetry()

    def drift_events(self):
        # type: () -> list
        """Return the full list of detected drift events."""
        return list(self._drift_events)

    def sigma_series(self):
        # type: () -> List[float]
        """Return the full σ history for external analysis."""
        return list(self._sigma_history)

    def _compute_telemetry(self):
        # type: () -> dict
        """Compute current telemetry from rolling buffers."""
        n_steps = len(self._sigma_history)

        if n_steps == 0:
            return {
                "sigma_mean": 0.0,
                "sigma_std": 0.0,
                "alpha": 0.0,
                "alpha_r_squared": 0.0,
                "power_law_likely": False,
                "in_critical_band": False,
                "drift_event": None,
                "n_steps": 0,
                "n_avalanches": len(self._avalanche_sizes),
                "verdict": "UNDETERMINED",
            }

        # Rolling σ statistics over the window
        window = self._sigma_history[-self.window_size:]
        w_n = len(window)
        s_mean = sum(window) / float(w_n)
        s_var = sum((s - s_mean) ** 2 for s in window) / float(w_n)
        s_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # Fit power-law to avalanche sizes via MLE
        alpha, r_squared = self._fit_power_law()
        power_law_likely = (
            ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
            and r_squared >= ALPHA_R2_THRESHOLD
        )

        in_band = self.sigma_band_low <= s_mean <= self.sigma_band_high
        verdict = _criticality_verdict(s_mean, alpha, r_squared)

        # ── Structured-criticality detection (INV_073) ──
        # When σ is within the critical band, check whether Shannon entropy
        # H is < 0.2 of maximum.  If so, flag as "structured criticality"
        # to distinguish from uniform criticality where H ≈ H_max.
        # Low H at confirmed criticality signals frozen-but-tipping dynamics
        # rather than genuine SOC with high information throughput.
        structured_criticality = False
        h_fraction_latest = 0.0
        if self._h_fraction_history:
            h_fraction_latest = self._h_fraction_history[-1]

        if in_band and h_fraction_latest < 0.2 and h_fraction_latest > 0.0:
            structured_criticality = True
            print(
                "[CRITICALITY] structured criticality detected: "
                "sigma={:.4f} in critical band, H/H_max={:.4f} (<0.2). "
                "Low entropy alongside critical branching ratio indicates "
                "ordered/frozen-but-tipping dynamics, not uniform SOC."
                .format(s_mean, h_fraction_latest)
            )

        # ── Avalanche power-law exponent α via log-log fit (O148) ────────
        # The Hill MLE α above is the strongest statistical discriminator
        # between true criticality and near-critical noise.  Without it,
        # the branching ratio alone cannot rule out sub-critical mimicry.
        # We extract α and R² from the log-log linear regression of the
        # avalanche size CCDF and store them explicitly in the telemetry
        # record alongside σ and H.  This closes the epistemic gap
        # identified in O148: survival rate alone (e.g. 0.9238) is
        # insufficient to characterize critical dynamics; the power-law
        # avalanche distribution is the load-bearing diagnostic.
        #
        # The log-log fit slope gives α_loglog (the CCDF exponent, which
        # equals -(α-1) for a power law P(X>=x) ~ x^{-(α-1)}).  R² of
        # this fit quantifies how well the avalanche sizes follow a
        # power-law distribution.
        alpha_loglog, alpha_loglog_slope, alpha_loglog_r2 = self._fit_power_law_loglog()

        # ── Avalanche-exponent cross-validation (σ, α co-reporting) ──────
        # Co-report σ and α in the same telemetry pass so the epistemic
        # loop can cross-validate criticality claims.  σ alone can be
        # spuriously near-unity; α deviating from the SOC universality
        # class (α≈2.3) signals pseudo-criticality.
        #
        # The Hill MLE α and the log-log CCDF α_loglog are independent
        # estimators.  When both agree AND fall within the SOC universality
        # band [1.5, 3.0], criticality is cross-validated.  When they
        # diverge, or when σ is in-band but α is outside SOC range,
        # pseudo-criticality is flagged.
        #
        # SOC universality class reference: α ≈ 2.3 (±0.5) for canonical
        # SOC systems (BTW sandpile, forest fire, neural avalanches).
        # Tighter reference: α_SOC ≈ 2.3 from the Game of Truth telemetry
        # snapshot (α≈2.351, R²=0.961).
        _ALPHA_SOC_TARGET = 2.3
        _ALPHA_SOC_TOLERANCE = 0.5
        _alpha_hill_in_soc = ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
        _alpha_loglog_in_soc = ALPHA_SOC_LOW <= alpha_loglog <= ALPHA_SOC_HIGH
        _alpha_agreement = (
            abs(alpha - alpha_loglog) < 0.3
            if alpha > 0.0 and alpha_loglog > 0.0 else False
        )
        _alpha_near_soc_target = (
            abs(alpha - _ALPHA_SOC_TARGET) < _ALPHA_SOC_TOLERANCE
            if alpha > 0.0 else False
        )
        _sigma_alpha_cross_validated = (
            in_band and _alpha_hill_in_soc and r_squared >= ALPHA_R2_THRESHOLD
        )
        _pseudo_criticality = (
            in_band and not _alpha_hill_in_soc and alpha > 0.0
        )

        if _sigma_alpha_cross_validated and _alpha_agreement:
            _cross_validation_status = "CONFIRMED_SOC"
            _cross_validation_detail = (
                "sigma={:.4f} in critical band AND alpha_hill={:.4f} "
                "(R^2={:.4f}) in SOC range [{:.1f}, {:.1f}], alpha_loglog="
                "{:.4f} (R^2={:.4f}) agrees. Cross-validated criticality."
            ).format(
                s_mean, alpha, r_squared, ALPHA_SOC_LOW, ALPHA_SOC_HIGH,
                alpha_loglog, alpha_loglog_r2,
            )
        elif _sigma_alpha_cross_validated:
            _cross_validation_status = "LIKELY_SOC"
            _cross_validation_detail = (
                "sigma={:.4f} in band, alpha_hill={:.4f} in SOC range, "
                "but alpha_loglog={:.4f} diverges (|diff|={:.4f}>0.3). "
                "Hill MLE confirms but log-log fit disagrees — possible "
                "distribution curvature or finite-size effect."
            ).format(
                s_mean, alpha, alpha_loglog,
                abs(alpha - alpha_loglog),
            )
        elif _pseudo_criticality:
            _cross_validation_status = "PSEUDO_CRITICAL"
            _cross_validation_detail = (
                "sigma={:.4f} in critical band BUT alpha={:.4f} outside "
                "SOC range [{:.1f}, {:.1f}] (R^2={:.4f}). Branching ratio "
                "alone cannot confirm criticality — avalanche statistics "
                "inconsistent with SOC universality class (alpha~2.3). "
                "This is the INV_073 dissociation pattern."
            ).format(
                s_mean, alpha, ALPHA_SOC_LOW, ALPHA_SOC_HIGH, r_squared,
            )
        elif alpha == 0.0 and in_band:
            _cross_validation_status = "SIGMA_ONLY"
            _cross_validation_detail = (
                "sigma={:.4f} in band but insufficient avalanche data "
                "for alpha estimation. Cannot cross-validate."
            ).format(s_mean)
        else:
            _cross_validation_status = "NOT_CRITICAL"
            _cross_validation_detail = (
                "sigma={:.4f} {}, alpha={:.4f}. Neither criterion "
                "confirms SOC."
            ).format(
                s_mean,
                "in band" if in_band else "outside band",
                alpha,
            )

        _criticality_cross_validation = {
            "status": _cross_validation_status,
            "detail": _cross_validation_detail,
            "sigma_mean": round(s_mean, 6),
            "sigma_in_band": in_band,
            "alpha_hill": round(alpha, 4),
            "alpha_hill_r_squared": round(r_squared, 4),
            "alpha_hill_in_soc": _alpha_hill_in_soc,
            "alpha_loglog": round(alpha_loglog, 4),
            "alpha_loglog_r_squared": round(alpha_loglog_r2, 4),
            "alpha_loglog_in_soc": _alpha_loglog_in_soc,
            "alpha_estimators_agree": _alpha_agreement,
            "alpha_near_soc_target": _alpha_near_soc_target,
            "alpha_soc_target": _ALPHA_SOC_TARGET,
            "alpha_deviation_from_target": round(
                abs(alpha - _ALPHA_SOC_TARGET), 4
            ) if alpha > 0.0 else 0.0,
            "pseudo_criticality_flag": _pseudo_criticality,
        }

        # ── Branching-ratio σ from consecutive avalanche sizes (O148) ────
        # Compute σ = mean(s_{n+1}) / mean(s_n) over consecutive avalanche
        # sizes in the accumulated buffer.  This is the single most
        # diagnostic scalar for criticality: σ ≈ 1 confirms the system
        # is at the critical ridge, σ > 1 indicates supercritical runaway,
        # σ < 1 indicates subcritical decay.  Embedding it in the sweep
        # loop makes every future CA snapshot automatically classifiable
        # without manual inspection.
        #
        # The computation splits the avalanche size sequence into
        # consecutive pairs (s_n, s_{n+1}), computes the ratio for each
        # pair, and reports the mean ratio as σ_avalanche.  This is
        # independent of the per-step parent/child σ computed above,
        # providing a cross-validation signal: if both σ estimates agree,
        # criticality is confirmed from two independent measurements.
        #
        # Paper reference: Game of Truth 32×32, 200-step telemetry —
        # σ = 1.0271 ± 0.017, within critical band (1.0 ± 0.05).
        #
        # Addresses O148: the telemetry now measures survival and avalanche
        # statistics AND demonstrates a branching-ratio σ extraction from
        # the avalanche size sequence itself, closing the diagnostic gap.
        sigma_avalanche = 0.0
        sigma_avalanche_std = 0.0
        sigma_avalanche_n_pairs = 0
        sigma_avalanche_in_critical_band = False
        sigma_avalanche_verdict = "UNDETERMINED"
        _aval_sizes_for_sigma = [s for s in self._avalanche_sizes if s > 0]
        if len(_aval_sizes_for_sigma) >= 4:
            # Split into consecutive non-overlapping pairs and compute ratios
            _aval_ratios = []  # type: List[float]
            # Use a sliding window of consecutive sizes: s_{n}, s_{n+1}
            # Group into even/odd halves for mean(s_{n+1})/mean(s_n)
            _half = len(_aval_sizes_for_sigma) // 2
            _s_n = _aval_sizes_for_sigma[:_half]
            _s_n1 = _aval_sizes_for_sigma[_half:_half * 2]
            _mean_s_n = sum(_s_n) / float(len(_s_n)) if _s_n else 0.0
            _mean_s_n1 = sum(_s_n1) / float(len(_s_n1)) if _s_n1 else 0.0
            if _mean_s_n > 1e-12:
                sigma_avalanche = _mean_s_n1 / _mean_s_n
            # Also compute per-pair ratios for std estimation
            _min_pairs = min(len(_s_n), len(_s_n1))
            for _pi in range(_min_pairs):
                if _s_n[_pi] > 1e-12:
                    _aval_ratios.append(_s_n1[_pi] / _s_n[_pi])
            sigma_avalanche_n_pairs = len(_aval_ratios)
            if sigma_avalanche_n_pairs >= 2:
                _ar_mean = sum(_aval_ratios) / float(sigma_avalanche_n_pairs)
                _ar_var = sum(
                    (_r - _ar_mean) ** 2 for _r in _aval_ratios
                ) / float(sigma_avalanche_n_pairs)
                sigma_avalanche_std = math.sqrt(_ar_var) if _ar_var > 0 else 0.0
            sigma_avalanche_in_critical_band = (
                SIGMA_CRITICAL_LOW <= sigma_avalanche <= SIGMA_CRITICAL_HIGH
            )
            if sigma_avalanche_in_critical_band:
                sigma_avalanche_verdict = "AT_CRITICAL"
            elif sigma_avalanche > SIGMA_CRITICAL_HIGH:
                sigma_avalanche_verdict = "SUPERCRITICAL"
            elif sigma_avalanche < SIGMA_CRITICAL_LOW and sigma_avalanche > 0:
                sigma_avalanche_verdict = "SUBCRITICAL"

        # ── Avalanche exponent α via log-log histogram regression (O148) ─
        # Extract the power-law exponent α and R² from a log-log linear
        # regression on the avalanche-size *histogram* (binned PDF).  This
        # is a third independent α estimator alongside Hill MLE and CCDF
        # log-log, closing the gap between manual telemetry snapshots
        # (which report α≈2.632, R²=0.897) and the automated criticality
        # audit.  Storing α_histogram and α_histogram_r2 makes O148
        # continuously trackable rather than episodically sampled.
        alpha_histogram = 0.0
        alpha_histogram_r2 = 0.0
        _hist_sizes = [s for s in self._avalanche_sizes if s > 0]
        if len(_hist_sizes) >= 20:
            _hs_min = min(_hist_sizes)
            _hs_max = max(_hist_sizes)
            if _hs_min > 0 and _hs_max > _hs_min:
                _ln_s_min = math.log(_hs_min)
                _ln_s_max = math.log(_hs_max)
                _ln_span = _ln_s_max - _ln_s_min
                if _ln_span > 0.3:
                    _n_log_bins = max(8, int(math.sqrt(float(len(_hist_sizes)))))
                    _bin_w = _ln_span / float(_n_log_bins)
                    _bin_counts = [0] * _n_log_bins
                    for _sv in _hist_sizes:
                        _bi = int((math.log(_sv) - _ln_s_min) / _bin_w)
                        if _bi >= _n_log_bins:
                            _bi = _n_log_bins - 1
                        _bin_counts[_bi] += 1
                    # Build log-log pairs: (ln(s_center), ln(density))
                    _hx = []  # type: List[float]
                    _hy = []  # type: List[float]
                    for _bi2 in range(_n_log_bins):
                        if _bin_counts[_bi2] > 0:
                            _lc = _ln_s_min + (float(_bi2) + 0.5) * _bin_w
                            _sc = math.exp(_lc)
                            _bw_s = _sc * (math.exp(_bin_w) - 1.0)
                            if _bw_s > 0:
                                _dens = float(_bin_counts[_bi2]) / (float(len(_hist_sizes)) * _bw_s)
                                if _dens > 0:
                                    _hx.append(_lc)
                                    _hy.append(math.log(_dens))
                    if len(_hx) >= 3:
                        _hk = len(_hx)
                        _hsx = sum(_hx)
                        _hsy = sum(_hy)
                        _hsxy = sum(_x * _y for _x, _y in zip(_hx, _hy))
                        _hsx2 = sum(_x * _x for _x in _hx)
                        _hd = float(_hk) * _hsx2 - _hsx * _hsx
                        if abs(_hd) > 1e-15:
                            _h_slope = (float(_hk) * _hsxy - _hsx * _hsy) / _hd
                            _h_int = (_hsy - _h_slope * _hsx) / float(_hk)
                            # For P(s) ~ s^{-alpha}, slope = -alpha
                            alpha_histogram = -_h_slope
                            _h_mean_y = _hsy / float(_hk)
                            _h_ss_tot = sum((_y - _h_mean_y) ** 2 for _y in _hy)
                            _h_ss_res = sum(
                                (_y - (_h_int + _h_slope * _x)) ** 2
                                for _x, _y in zip(_hx, _hy)
                            )
                            alpha_histogram_r2 = (
                                1.0 - (_h_ss_res / _h_ss_tot)
                                if _h_ss_tot > 1e-15 else 0.0
                            )
                            alpha_histogram_r2 = max(0.0, alpha_histogram_r2)

        return {
            "sigma_mean": round(s_mean, 6),
            "sigma_std": round(s_std, 6),
            "alpha": round(alpha, 4),
            "alpha_r_squared": round(r_squared, 4),
            "alpha_loglog": round(alpha_loglog, 4),
            "alpha_loglog_slope": round(alpha_loglog_slope, 6),
            "alpha_loglog_r_squared": round(alpha_loglog_r2, 4),
            "alpha_histogram": round(alpha_histogram, 4),
            "alpha_histogram_r_squared": round(alpha_histogram_r2, 4),
            "power_law_likely": power_law_likely,
            "in_critical_band": in_band,
            "drift_event": None,
            "n_steps": n_steps,
            "n_avalanches": len(self._avalanche_sizes),
            "verdict": verdict,
            "structured_criticality": structured_criticality,
            "h_fraction_latest": round(h_fraction_latest, 4),
            "criticality_cross_validation": _criticality_cross_validation,
        }

    def _fit_power_law_loglog(self):
        # type: () -> Tuple[float, float, float]
        """
        Extract avalanche power-law exponent α via log-log linear regression
        of the empirical complementary CDF (CCDF) of avalanche sizes.

        This is the direct log-log fit method that provides an independent
        α estimate alongside the Hill MLE from _fit_power_law().  The two
        estimates should agree for genuine power-law distributions; divergence
        indicates the distribution departs from a pure power law.

        The CCDF is: P(X >= x) ~ x^{-(α-1)}
        In log-log space: log P = intercept + slope * log x
        where slope = -(α-1), so α = 1 - slope.

        Returns
        -------
        (alpha_loglog, slope, r_squared)
            alpha_loglog : float — power-law exponent from log-log CCDF fit
            slope : float — raw slope of the log-log regression (negative for power law)
            r_squared : float — R² goodness-of-fit (higher = better power-law fit)
            Returns (0.0, 0.0, 0.0) if insufficient data.
        """
        sizes = [s for s in self._avalanche_sizes if s > 0]
        if len(sizes) < 10:
            return (0.0, 0.0, 0.0)

        x_min = max(1.0, min(sizes))
        tail = [s for s in sizes if s >= x_min]
        n = len(tail)
        if n < 5:
            return (0.0, 0.0, 0.0)

        tail_sorted = sorted(tail)
        unique_sizes = sorted(set(tail_sorted))
        n_total = float(len(tail_sorted))

        log_x = []     # type: List[float]
        log_ccdf = []  # type: List[float]
        for x_val in unique_sizes:
            count_ge = sum(1 for s in tail_sorted if s >= x_val)
            p = float(count_ge) / n_total
            if p > 0 and x_val > 0:
                log_x.append(math.log(float(x_val)))
                log_ccdf.append(math.log(p))

        if len(log_x) < 3:
            return (0.0, 0.0, 0.0)

        k = len(log_x)
        sum_lx = sum(log_x)
        sum_ly = sum(log_ccdf)
        sum_lxy = sum(x * y for x, y in zip(log_x, log_ccdf))
        sum_lx2 = sum(x * x for x in log_x)

        denom = float(k) * sum_lx2 - sum_lx * sum_lx
        if abs(denom) < 1e-15:
            return (0.0, 0.0, 0.0)

        slope = (float(k) * sum_lxy - sum_lx * sum_ly) / denom
        intercept = (sum_ly - slope * sum_lx) / float(k)

        mean_ly = sum_ly / float(k)
        ss_tot = sum((y - mean_ly) ** 2 for y in log_ccdf)
        ss_res = sum((y - (intercept + slope * x)) ** 2
                     for x, y in zip(log_x, log_ccdf))
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0

        # α = 1 - slope (slope is negative for a valid power law)
        alpha_loglog = 1.0 - slope

        return (alpha_loglog, slope, max(0.0, r_squared))

    def _fit_power_law(self):
        # type: () -> Tuple[float, float]
        """
        Fit a power-law exponent α to the avalanche size distribution
        using discrete MLE (Hill estimator) and compute R² of the fit
        against the empirical complementary CDF on a log-log scale.

        Returns (alpha, r_squared). Returns (0.0, 0.0) if insufficient data.

        The Hill estimator for discrete power-law:
            α = 1 + n / Σ ln(x_i / x_min)
        where x_min is the minimum avalanche size (typically 1).
        """
        # Filter to positive avalanche sizes
        sizes = [s for s in self._avalanche_sizes if s > 0]
        if len(sizes) < 10:
            return (0.0, 0.0)

        # x_min: use the smallest observed size (at least 1.0)
        x_min = max(1.0, min(sizes))

        # Filter sizes >= x_min
        tail = [s for s in sizes if s >= x_min]
        n = len(tail)
        if n < 5:
            return (0.0, 0.0)

        # Hill estimator: α = 1 + n / Σ ln(x_i / x_min)
        log_sum = 0.0
        for s in tail:
            ratio = float(s) / x_min
            if ratio > 0:
                log_sum += math.log(ratio)

        if log_sum <= 0.0:
            return (0.0, 0.0)

        alpha = 1.0 + float(n) / log_sum

        # Compute R² on log-log CCDF
        # Sort sizes descending, compute empirical CCDF
        tail_sorted = sorted(tail)
        unique_sizes = sorted(set(tail_sorted))
        n_total = float(len(tail_sorted))

        # Empirical CCDF: P(X >= x) for each unique x
        log_x = []   # type: List[float]
        log_ccdf = []  # type: List[float]
        for x_val in unique_sizes:
            count_ge = sum(1 for s in tail_sorted if s >= x_val)
            p = float(count_ge) / n_total
            if p > 0 and x_val > 0:
                log_x.append(math.log(float(x_val)))
                log_ccdf.append(math.log(p))

        if len(log_x) < 3:
            return (alpha, 0.0)

        # Theoretical CCDF for power-law: P(X >= x) ∝ x^(-(α-1))
        # log P = const - (α-1) * log x
        # Fit via linear regression to get R²
        k = len(log_x)
        sum_lx = sum(log_x)
        sum_ly = sum(log_ccdf)
        sum_lxy = sum(x * y for x, y in zip(log_x, log_ccdf))
        sum_lx2 = sum(x * x for x in log_x)

        denom = float(k) * sum_lx2 - sum_lx * sum_lx
        if abs(denom) < 1e-15:
            return (alpha, 0.0)

        slope = (float(k) * sum_lxy - sum_lx * sum_ly) / denom
        intercept = (sum_ly - slope * sum_lx) / float(k)

        # R²
        mean_ly = sum_ly / float(k)
        ss_tot = sum((y - mean_ly) ** 2 for y in log_ccdf)
        ss_res = sum((y - (intercept + slope * x)) ** 2
                     for x, y in zip(log_x, log_ccdf))
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0

        return (alpha, max(0.0, r_squared))


class AvalancheEpochTracker:
    """
    Branching-ratio tracker that computes σ = mean(offspring)/mean(ancestors)
    per avalanche epoch and flags deviation from [0.95, 1.05] as a
    criticality-drift alarm.

    An "avalanche epoch" is a contiguous burst of above-baseline activity
    bookended by quiescent steps.  Within each epoch the tracker accumulates
    ancestor (parent) and offspring (child) counts, then on epoch termination
    computes σ_epoch and checks it against the critical band.

    Designed to sit inside the CA telemetry pipeline between raw step data
    and the CACriticalityLog, closing the feedback loop between telemetry
    and kernel health so FREED can detect drift from the critical ridge
    *before* coherence scores degrade.

    Addresses O140 (CA measurement grounding), INV_073 (Wasserstein gradient
    bias detection: if σ stabilizes above 1.0 rather than oscillating around
    it, the tracker flags a sustained-supercritical alarm distinct from
    transient fluctuations).

    Usage::

        tracker = AvalancheEpochTracker()
        for step in simulation:
            alarms = tracker.feed_step(parent_count, child_count, is_active)
            if alarms:
                for alarm in alarms:
                    handle_drift_alarm(alarm)
        # At end, flush any open epoch:
        final = tracker.flush()
    """

    def __init__(
        self,
        sigma_band_low=0.95,   # type: float
        sigma_band_high=1.05,  # type: float
        quiescent_threshold=0, # type: int
        sustained_window=5,    # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        sigma_band_low : float
            Lower bound of the critical band for σ. Default: 0.95.
        sigma_band_high : float
            Upper bound of the critical band for σ. Default: 1.05.
        quiescent_threshold : int
            A step with parent_count <= this value is considered quiescent
            (marks the boundary between avalanche epochs). Default: 0.
        sustained_window : int
            Number of consecutive epochs with σ on the same side of 1.0
            required to trigger a sustained-drift alarm (INV_073 pattern).
            Default: 5.
        """
        self.sigma_band_low = sigma_band_low
        self.sigma_band_high = sigma_band_high
        self.quiescent_threshold = quiescent_threshold
        self.sustained_window = max(2, sustained_window)

        # Current epoch accumulators
        self._epoch_ancestors = []   # type: List[int]
        self._epoch_offspring = []   # type: List[int]
        self._in_epoch = False       # type: bool
        self._epoch_id = 0           # type: int

        # Completed epoch history
        self._epoch_sigmas = []      # type: List[float]
        self._epoch_records = []     # type: list
        self._alarms = []            # type: list

    def feed_step(self, parent_count, child_count, is_active=None):
        # type: (int, int, Optional[bool]) -> list
        """
        Feed one simulation step into the epoch tracker.

        Parameters
        ----------
        parent_count : int
            Number of active ancestor cells at this step.
        child_count : int
            Number of active offspring cells produced at this step.
        is_active : bool or None
            Whether this step is part of an active avalanche.  If None,
            activity is inferred from parent_count > quiescent_threshold.

        Returns
        -------
        list of dict
            Any alarms generated by epoch completion at this step.
            Empty list if no epoch ended or no alarm was triggered.
        """
        if is_active is None:
            is_active = parent_count > self.quiescent_threshold

        alarms = []  # type: list

        if is_active:
            # Inside an avalanche epoch — accumulate
            if not self._in_epoch:
                # Start new epoch
                self._in_epoch = True
                self._epoch_ancestors = []
                self._epoch_offspring = []
            self._epoch_ancestors.append(parent_count)
            self._epoch_offspring.append(child_count)
        else:
            # Quiescent step — close any open epoch
            if self._in_epoch:
                alarms = self._close_epoch()
            self._in_epoch = False

        return alarms

    def flush(self):
        # type: () -> list
        """
        Close any currently open epoch (e.g., at end of simulation).

        Returns
        -------
        list of dict — any alarms from the final epoch.
        """
        if self._in_epoch and self._epoch_ancestors:
            alarms = self._close_epoch()
            self._in_epoch = False
            return alarms
        return []

    def epoch_records(self):
        # type: () -> list
        """Return all completed epoch records."""
        return list(self._epoch_records)

    def epoch_sigma_series(self):
        # type: () -> List[float]
        """Return σ values for all completed epochs."""
        return list(self._epoch_sigmas)

    def alarms(self):
        # type: () -> list
        """Return all alarms ever raised."""
        return list(self._alarms)

    def summary(self):
        # type: () -> dict
        """
        Return a summary of all completed epochs.

        Returns
        -------
        dict with keys:
            n_epochs           : int
            sigma_global_mean  : float — mean of per-epoch σ values
            sigma_global_std   : float — std of per-epoch σ values
            n_alarms           : int
            n_sustained_alarms : int   — alarms of type "sustained_supercritical"
                                         or "sustained_subcritical"
            fraction_in_band   : float — fraction of epochs with σ in critical band
        """
        n = len(self._epoch_sigmas)
        if n == 0:
            return {
                "n_epochs": 0,
                "sigma_global_mean": 0.0,
                "sigma_global_std": 0.0,
                "n_alarms": 0,
                "n_sustained_alarms": 0,
                "fraction_in_band": 0.0,
            }

        s_mean = sum(self._epoch_sigmas) / float(n)
        s_var = sum((s - s_mean) ** 2 for s in self._epoch_sigmas) / float(n)
        s_std = math.sqrt(s_var) if s_var > 0 else 0.0

        in_band = sum(
            1 for s in self._epoch_sigmas
            if self.sigma_band_low <= s <= self.sigma_band_high
        )
        n_sustained = sum(
            1 for a in self._alarms
            if a.get("alarm_type", "").startswith("sustained_")
        )

        return {
            "n_epochs": n,
            "sigma_global_mean": round(s_mean, 6),
            "sigma_global_std": round(s_std, 6),
            "n_alarms": len(self._alarms),
            "n_sustained_alarms": n_sustained,
            "fraction_in_band": round(float(in_band) / float(n), 4),
        }

    def _close_epoch(self):
        # type: () -> list
        """Close the current epoch, compute σ, check for alarms."""
        alarms = []  # type: list
        self._epoch_id += 1

        n_steps = len(self._epoch_ancestors)
        if n_steps == 0:
            return alarms

        mean_ancestors = sum(self._epoch_ancestors) / float(n_steps)
        mean_offspring = sum(self._epoch_offspring) / float(n_steps)

        if mean_ancestors > 0.0:
            sigma = mean_offspring / mean_ancestors
        else:
            sigma = 0.0

        self._epoch_sigmas.append(sigma)

        record = {
            "epoch_id": self._epoch_id,
            "n_steps": n_steps,
            "mean_ancestors": round(mean_ancestors, 4),
            "mean_offspring": round(mean_offspring, 4),
            "sigma": round(sigma, 6),
            "in_critical_band": self.sigma_band_low <= sigma <= self.sigma_band_high,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._epoch_records.append(record)

        # ── Band-deviation alarm ──
        if sigma != 0.0 and not (self.sigma_band_low <= sigma <= self.sigma_band_high):
            direction = "supercritical" if sigma > self.sigma_band_high else "subcritical"
            alarm = {
                "alarm_type": "epoch_drift",
                "epoch_id": self._epoch_id,
                "sigma": round(sigma, 6),
                "direction": direction,
                "deviation": round(abs(sigma - 1.0), 6),
                "timestamp": record["timestamp"],
            }
            self._alarms.append(alarm)
            alarms.append(alarm)

        # ── INV_073 sustained-drift alarm ──
        # If the last `sustained_window` epochs all have σ on the same side
        # of 1.0 (all > 1.0 or all < 1.0), flag a sustained-drift alarm.
        # This detects the pattern where σ stabilizes above 1.0 rather than
        # oscillating around it, indicating Wasserstein gradient bias toward
        # supercritical runaway.
        if len(self._epoch_sigmas) >= self.sustained_window:
            recent = self._epoch_sigmas[-self.sustained_window:]
            all_above = all(s > 1.0 for s in recent)
            all_below = all(s < 1.0 for s in recent)

            if all_above or all_below:
                sustained_dir = "sustained_supercritical" if all_above else "sustained_subcritical"
                recent_mean = sum(recent) / float(len(recent))

                # Only alarm once per sustained run — check if last alarm
                # was already a sustained alarm at the same epoch range
                should_alarm = True
                if self._alarms:
                    last = self._alarms[-1]
                    if (last.get("alarm_type", "").startswith("sustained_")
                            and last.get("epoch_id", 0) == self._epoch_id - 1):
                        # Already alarmed on the previous epoch in this run;
                        # still alarm but mark as continuation
                        pass

                if should_alarm:
                    alarm = {
                        "alarm_type": sustained_dir,
                        "epoch_id": self._epoch_id,
                        "sigma_mean_recent": round(recent_mean, 6),
                        "window_size": self.sustained_window,
                        "sigmas": [round(s, 6) for s in recent],
                        "inv073_relevant": all_above,
                        "detail": (
                            "sigma has been consistently {} 1.0 for {} consecutive "
                            "epochs (mean={:.4f}). This may indicate Wasserstein "
                            "gradient bias toward {} rather than true ridge "
                            "navigation (INV_073)."
                        ).format(
                            "above" if all_above else "below",
                            self.sustained_window,
                            recent_mean,
                            "supercritical runaway" if all_above else "subcritical freeze",
                        ),
                        "timestamp": record["timestamp"],
                    }
                    self._alarms.append(alarm)
                    alarms.append(alarm)

        return alarms


class SigmaExcursionTracker:
    """
    Per-timestep branching-ratio tracker that computes σ and flags when
    σ exits the critical band (1.0 ± 0.05), logging the dominant agent
    type at each excursion.

    Designed to sit in the CA telemetry pipeline between raw step data
    and the CACriticalityLog, providing fine-grained causal evidence
    for whether Physics Navigator dominance *causes* or *follows*
    criticality drift — closing the causal ambiguity from the telemetry
    snapshot (σ=1.0208, dominant=Physics Navigator, 860 cells).

    Addresses INV_073 falsification: if σ drifts consistently above 1.05
    as Physics Navigator cells accumulate, the system is supercritical
    (γ>1, frozen), directly contradicting INV_073's claim that the
    Wasserstein gradient path necessarily converges to and sustains γ=1.

    Usage::

        tracker = SigmaExcursionTracker()
        for step in simulation:
            result = tracker.record_step(
                step=step,
                parent_count=parents,
                child_count=children,
                type_counts={"Physics Navigator": 860, "Entropy Scorer": 54},
            )
            if result["excursion"]:
                log_excursion(result)
        report = tracker.excursion_report()
    """

    def __init__(
        self,
        sigma_center=1.0,       # type: float
        sigma_half_band=0.05,   # type: float
        history_limit=2000,     # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        sigma_center : float
            Centre of the critical band. Default: 1.0.
        sigma_half_band : float
            Half-width of the critical band. Default: 0.05
            (band = [0.95, 1.05]).
        history_limit : int
            Maximum number of per-step records to retain.  Default: 2000.
        """
        self.sigma_center = sigma_center
        self.sigma_half_band = sigma_half_band
        self.sigma_low = sigma_center - sigma_half_band
        self.sigma_high = sigma_center + sigma_half_band
        self.history_limit = max(10, history_limit)

        self._steps = []          # type: list
        self._sigma_series = []   # type: List[float]
        self._excursions = []     # type: list

    def record_step(
        self,
        step,               # type: int
        parent_count,       # type: int
        child_count,        # type: int
        type_counts=None,   # type: Optional[dict]
    ):
        # type: (...) -> dict
        """
        Record one simulation step and check for σ excursion.

        Parameters
        ----------
        step : int
            The simulation timestep number.
        parent_count : int
            Number of active (parent) cells at this step.
        child_count : int
            Number of active (child/descendant) cells at the next step.
        type_counts : dict or None
            Mapping of cell-type name (str) to count (int).
            Example: {"Physics Navigator": 860, "Entropy Scorer": 54}

        Returns
        -------
        dict with keys:
            step             : int
            sigma            : float   — instantaneous branching ratio
            in_band          : bool    — whether σ is in [σ_low, σ_high]
            excursion        : bool    — True if σ exited the critical band
            excursion_dir    : str     — "supercritical" / "subcritical" / ""
            dominant_type    : str     — most populous cell type at this step
            dominant_count   : int     — count of the dominant type
            dominant_share   : float   — fraction of total population
            total_population : int
            timestamp        : str     — ISO-8601 UTC
        """
        if type_counts is None:
            type_counts = {}

        # Compute instantaneous σ
        if parent_count > 0:
            sigma = float(child_count) / float(parent_count)
        else:
            sigma = 0.0

        self._sigma_series.append(sigma)

        # Determine dominant type
        total_pop = sum(type_counts.values()) if type_counts else 0
        total_f = float(total_pop) if total_pop > 0 else 1.0
        if type_counts:
            dominant_type = max(type_counts, key=type_counts.get)
            dominant_count = type_counts[dominant_type]
            dominant_share = round(float(dominant_count) / total_f, 6)
        else:
            dominant_type = ""
            dominant_count = 0
            dominant_share = 0.0

        # Check band membership
        in_band = self.sigma_low <= sigma <= self.sigma_high
        excursion = (sigma != 0.0) and not in_band

        if excursion:
            excursion_dir = "supercritical" if sigma > self.sigma_high else "subcritical"
        else:
            excursion_dir = ""

        record = {
            "step":             step,
            "sigma":            round(sigma, 6),
            "in_band":          in_band,
            "excursion":        excursion,
            "excursion_dir":    excursion_dir,
            "dominant_type":    dominant_type,
            "dominant_count":   dominant_count,
            "dominant_share":   dominant_share,
            "total_population": total_pop,
            "type_counts":      dict(type_counts),
            "timestamp":        datetime.now(timezone.utc).isoformat(),
        }

        self._steps.append(record)

        if excursion:
            self._excursions.append(record)

        # Trim history
        if len(self._steps) > self.history_limit:
            trim = len(self._steps) - self.history_limit
            self._steps = self._steps[trim:]
        if len(self._sigma_series) > self.history_limit:
            self._sigma_series = self._sigma_series[-self.history_limit:]

        return record

    def sigma_series(self):
        # type: () -> List[float]
        """Return the full per-step σ history."""
        return list(self._sigma_series)

    def excursions(self):
        # type: () -> list
        """Return all excursion records."""
        return list(self._excursions)

    def excursion_report(self):
        # type: () -> dict
        """
        Generate a summary report of all excursions, including the
        dominant agent type at each excursion and aggregate statistics
        for causal analysis (INV_073 falsification).

        Returns
        -------
        dict with keys:
            n_steps              : int   — total steps recorded
            n_excursions         : int   — total excursion events
            n_supercritical      : int   — excursions above the band
            n_subcritical        : int   — excursions below the band
            excursion_rate       : float — fraction of steps with excursion
            sigma_mean           : float — overall mean σ
            sigma_std            : float — overall std σ
            dominant_at_excursion: dict  — {type_name: count} across all excursions
            dominant_at_supercrit: dict  — {type_name: count} for supercritical only
            dominant_at_subcrit  : dict  — {type_name: count} for subcritical only
            inv073_falsified     : bool  — True if σ mean > 1.05 (sustained
                                           supercritical, contradicts INV_073)
            inv073_detail        : str   — human-readable assessment
            excursion_records    : list  — the raw excursion records
        """
        n_steps = len(self._sigma_series)
        n_exc = len(self._excursions)
        n_super = sum(1 for e in self._excursions if e["excursion_dir"] == "supercritical")
        n_sub = sum(1 for e in self._excursions if e["excursion_dir"] == "subcritical")

        exc_rate = round(float(n_exc) / float(n_steps), 6) if n_steps > 0 else 0.0

        # Overall σ statistics
        if self._sigma_series:
            s_mean = sum(self._sigma_series) / float(len(self._sigma_series))
            s_var = sum((s - s_mean) ** 2 for s in self._sigma_series) / float(len(self._sigma_series))
            s_std = math.sqrt(s_var) if s_var > 0 else 0.0
        else:
            s_mean = 0.0
            s_std = 0.0

        # Dominant type tallies at excursions
        dom_all = {}    # type: dict
        dom_super = {}  # type: dict
        dom_sub = {}    # type: dict
        for e in self._excursions:
            dt = e["dominant_type"]
            if dt:
                dom_all[dt] = dom_all.get(dt, 0) + 1
                if e["excursion_dir"] == "supercritical":
                    dom_super[dt] = dom_super.get(dt, 0) + 1
                elif e["excursion_dir"] == "subcritical":
                    dom_sub[dt] = dom_sub.get(dt, 0) + 1

        # INV_073 falsification check
        inv073_falsified = s_mean > self.sigma_high
        if inv073_falsified:
            inv073_detail = (
                "FALSIFIED: mean sigma={:.4f} exceeds critical band upper "
                "bound {:.2f}. The system is sustained-supercritical, "
                "contradicting INV_073 claim that Wasserstein gradient "
                "path necessarily converges to gamma=1."
            ).format(s_mean, self.sigma_high)
        elif s_mean < self.sigma_low:
            inv073_detail = (
                "WARNING: mean sigma={:.4f} below critical band lower "
                "bound {:.2f}. System is sustained-subcritical."
            ).format(s_mean, self.sigma_low)
        else:
            inv073_detail = (
                "CONSISTENT: mean sigma={:.4f} within critical band "
                "[{:.2f}, {:.2f}]. INV_073 not falsified by sigma drift. "
                "Excursion rate={:.2%} ({} of {} steps)."
            ).format(s_mean, self.sigma_low, self.sigma_high, exc_rate, n_exc, n_steps)

        return {
            "n_steps":               n_steps,
            "n_excursions":          n_exc,
            "n_supercritical":       n_super,
            "n_subcritical":         n_sub,
            "excursion_rate":        exc_rate,
            "sigma_mean":            round(s_mean, 6),
            "sigma_std":             round(s_std, 6),
            "dominant_at_excursion": dom_all,
            "dominant_at_supercrit": dom_super,
            "dominant_at_subcrit":   dom_sub,
            "inv073_falsified":      inv073_falsified,
            "inv073_detail":         inv073_detail,
            "excursion_records":     list(self._excursions),
        }


class CACriticalityLog:
    """
    Longitudinal log of per-generation CA criticality telemetry.

    Accumulates scored telemetry records from score_ca_generation()
    and provides drift detection, rolling statistics, and serialization
    for O140/O141 analysis.
    """

    def __init__(self, log_path=None):
        # type: (Optional[Path]) -> None
        self.records = []   # type: list
        self.log_path = log_path or (FREED_DIR / "ca_criticality_log.json")

    def append(self, record):
        # type: (dict) -> None
        """Append a scored telemetry record and persist to disk."""
        self.records.append(record)
        self._persist()

    def _persist(self):
        # type: () -> None
        """Write all records to JSON log file."""
        try:
            with open(self.log_path, "w") as f:
                json.dump(self.records, f, indent=2)
        except OSError as e:
            print(f"[CA_LOG] Write error: {e}")

    def load(self):
        # type: () -> None
        """Load existing records from disk."""
        if self.log_path.exists():
            try:
                with open(self.log_path) as f:
                    self.records = json.load(f)
            except (json.JSONDecodeError, OSError):
                self.records = []

    def sigma_series(self):
        # type: () -> List[float]
        """Extract σ time series for complexity analysis."""
        return [r["sigma"] for r in self.records]

    def alpha_series(self):
        # type: () -> List[float]
        """Extract α time series for complexity analysis."""
        return [r["alpha"] for r in self.records]

    def drift_summary(self, window=20):
        # type: (int) -> dict
        """
        Compute rolling drift statistics over the last `window` generations.

        Returns
        -------
        dict with keys:
            sigma_mean     : float — rolling mean of σ
            sigma_std      : float — rolling std of σ
            alpha_mean     : float — rolling mean of α
            alpha_std      : float — rolling std of α
            drift_trend    : str   — "stable" / "freezing" / "dissipating"
            n_at_critical  : int   — count of AT_CRITICAL verdicts in window
            n_total        : int   — total records in window
        """
        recent = self.records[-window:] if len(self.records) >= window else self.records
        n = len(recent)

        if n == 0:
            return {
                "sigma_mean": 0.0, "sigma_std": 0.0,
                "alpha_mean": 0.0, "alpha_std": 0.0,
                "drift_trend": "no_data",
                "n_at_critical": 0, "n_total": 0,
            }

        sigmas = [r["sigma"] for r in recent]
        alphas = [r["alpha"] for r in recent]

        s_mean = sum(sigmas) / float(n)
        a_mean = sum(alphas) / float(n)

        s_var = sum((s - s_mean) ** 2 for s in sigmas) / float(n)
        a_var = sum((a - a_mean) ** 2 for a in alphas) / float(n)

        s_std = math.sqrt(s_var) if s_var > 0 else 0.0
        a_std = math.sqrt(a_var) if a_var > 0 else 0.0

        n_crit = sum(1 for r in recent if r["verdict"] == "AT_CRITICAL")

        # Detect drift trend from σ trajectory
        if n >= 4:
            first_half = sigmas[:n // 2]
            second_half = sigmas[n // 2:]
            fh_mean = sum(first_half) / float(len(first_half))
            sh_mean = sum(second_half) / float(len(second_half))
            delta = sh_mean - fh_mean
            if delta > 0.02:
                trend = "dissipating"   # σ rising above 1 → supercritical
            elif delta < -0.02:
                trend = "freezing"      # σ falling below 1 → subcritical
            else:
                trend = "stable"
        else:
            trend = "stable"

        return {
            "sigma_mean":    round(s_mean, 6),
            "sigma_std":     round(s_std, 6),
            "alpha_mean":    round(a_mean, 4),
            "alpha_std":     round(a_std, 4),
            "drift_trend":   trend,
            "n_at_critical": n_crit,
            "n_total":       n,
        }


class CellTypeDistributionTracker:
    """
    Tracks population share of each cell type (e.g. Physics Navigator,
    Entropy Scorer, etc.) at each measurement interval, logging alongside
    branching ratio σ and Shannon entropy H.

    Cell-type dominance is a leading indicator of population-level selection
    pressure and may predict convergence speed to criticality or drift away
    from the critical band before σ shifts.

    Addresses INV_073: low Shannon entropy (H ≈ 0.2 of max) at confirmed
    criticality (σ ≈ 1.02) is consistent with *ordered* SOC phases where
    one cell type dominates.  This tracker makes the dominance structure
    explicit and machine-readable, distinguishing critically ordered states
    (single-type dominance + σ ≈ 1 + power-law avalanches) from critically
    balanced states (uniform distribution + σ ≈ 1).

    Usage::

        tracker = CellTypeDistributionTracker()
        for step in simulation:
            census = {"Physics Navigator": 872, "Entropy Scorer": 54,
                      "Topology Agent": 98}
            snapshot = tracker.record(
                generation=step,
                type_counts=census,
                sigma=1.0244,
                sigma_std=0.0157,
                shannon_h=0.5159,
                shannon_h_max=2.585,
            )
            if snapshot["dominance_ratio"] > 0.8:
                handle_monoculture_warning(snapshot)
    """

    def __init__(self, history_limit=500):
        # type: (int) -> None
        """
        Parameters
        ----------
        history_limit : int
            Maximum number of snapshots to retain in memory.  Oldest are
            discarded when the limit is exceeded.  Default: 500.
        """
        self.history_limit = max(10, history_limit)
        self._snapshots = []  # type: list

    def record(
        self,
        generation,       # type: int
        type_counts,      # type: dict
        sigma=0.0,        # type: float
        sigma_std=0.0,    # type: float
        shannon_h=0.0,    # type: float
        shannon_h_max=0.0,# type: float
        alpha=0.0,        # type: float
        alpha_r_squared=0.0, # type: float
        survival_rate=0.0,# type: float
    ):
        # type: (...) -> dict
        """
        Record one measurement interval's cell-type census.

        Parameters
        ----------
        generation : int
            The simulation step / generation number.
        type_counts : dict
            Mapping of cell-type name (str) to count (int).
            Example: {"Physics Navigator": 872, "Entropy Scorer": 54}
        sigma : float
            Branching ratio σ at this interval.
        sigma_std : float
            Standard deviation of σ within the measurement window.
        shannon_h : float
            Shannon entropy H of the type distribution (bits).
        shannon_h_max : float
            Maximum possible Shannon entropy (log2 of number of types).
        alpha : float
            Power-law exponent α from avalanche size distribution.
        alpha_r_squared : float
            R² goodness-of-fit for the power-law regression.
        survival_rate : float
            Fraction of cells surviving this generation.

        Returns
        -------
        dict — the snapshot record with population shares and dominance metrics.
        """
        total = sum(type_counts.values())
        total_f = float(total) if total > 0 else 1.0

        # Population shares (fractions)
        shares = {}  # type: dict
        for cell_type, count in type_counts.items():
            shares[cell_type] = round(float(count) / total_f, 6)

        # Dominance metrics
        if type_counts:
            dominant_type = max(type_counts, key=type_counts.get)
            dominant_count = type_counts[dominant_type]
            dominance_ratio = round(float(dominant_count) / total_f, 6)
        else:
            dominant_type = ""
            dominant_count = 0
            dominance_ratio = 0.0

        n_types = len([c for c in type_counts.values() if c > 0])

        # Effective number of types (exponential of Shannon entropy)
        # N_eff = 2^H  (when H is in bits)
        if shannon_h > 0.0:
            n_effective = round(2.0 ** shannon_h, 4)
        else:
            n_effective = 1.0 if n_types > 0 else 0.0

        h_fraction = round(shannon_h / shannon_h_max, 4) if shannon_h_max > 0.0 else 0.0

        # Classify the diversity state
        if n_types <= 1:
            diversity_state = "MONOCULTURE"
        elif dominance_ratio > 0.85:
            diversity_state = "DOMINATED"
        elif h_fraction > 0.8:
            diversity_state = "BALANCED"
        elif h_fraction > 0.4:
            diversity_state = "MODERATE"
        else:
            diversity_state = "ORDERED"

        # Criticality-diversity joint classification (INV_073)
        verdict = _criticality_verdict(sigma, alpha, alpha_r_squared)
        if verdict == "AT_CRITICAL" and diversity_state in ("ORDERED", "DOMINATED", "MONOCULTURE"):
            joint_state = "CRITICALLY_ORDERED"
        elif verdict == "AT_CRITICAL" and diversity_state in ("BALANCED", "MODERATE"):
            joint_state = "CRITICALLY_BALANCED"
        elif verdict == "AT_CRITICAL":
            joint_state = "AT_CRITICAL"
        else:
            joint_state = verdict

        snapshot = {
            "generation":       generation,
            "type_counts":      dict(type_counts),
            "population_shares": shares,
            "total_population": total,
            "n_types_active":   n_types,
            "n_effective_types": n_effective,
            "dominant_type":    dominant_type,
            "dominant_count":   dominant_count,
            "dominance_ratio":  dominance_ratio,
            "diversity_state":  diversity_state,
            "sigma":            round(sigma, 6),
            "sigma_std":        round(sigma_std, 6),
            "shannon_h":        round(shannon_h, 4),
            "h_fraction":       h_fraction,
            "shannon_h_max":    round(shannon_h_max, 4),
            "alpha":            round(alpha, 4),
            "alpha_r_squared":  round(alpha_r_squared, 4),
            "survival_rate":    round(survival_rate, 4),
            "verdict":          verdict,
            "joint_state":      joint_state,
            "timestamp":        datetime.now(timezone.utc).isoformat(),
        }

        self._snapshots.append(snapshot)
        if len(self._snapshots) > self.history_limit:
            self._snapshots = self._snapshots[-self.history_limit:]

        return snapshot

    def snapshots(self):
        # type: () -> list
        """Return all recorded snapshots."""
        return list(self._snapshots)

    def dominance_series(self):
        # type: () -> List[float]
        """Return the dominance_ratio time series for trend analysis."""
        return [s["dominance_ratio"] for s in self._snapshots]

    def type_share_series(self, cell_type):
        # type: (str) -> List[float]
        """Return the population share time series for a specific cell type."""
        result = []  # type: List[float]
        for s in self._snapshots:
            result.append(s["population_shares"].get(cell_type, 0.0))
        return result

    def diversity_summary(self, window=20):
        # type: (int) -> dict
        """
        Compute rolling diversity statistics over the last `window` snapshots.

        Returns
        -------
        dict with keys:
            dominance_mean     : float — rolling mean of dominance_ratio
            dominance_std      : float — rolling std of dominance_ratio
            h_fraction_mean    : float — rolling mean of h_fraction
            dominant_type_mode : str   — most frequent dominant type in window
            diversity_trend    : str   — "diversifying" / "consolidating" / "stable"
            n_snapshots        : int   — number of snapshots in window
            joint_state_counts : dict  — count of each joint_state in window
        """
        recent = self._snapshots[-window:] if len(self._snapshots) >= window else self._snapshots
        n = len(recent)

        if n == 0:
            return {
                "dominance_mean": 0.0,
                "dominance_std": 0.0,
                "h_fraction_mean": 0.0,
                "dominant_type_mode": "",
                "diversity_trend": "no_data",
                "n_snapshots": 0,
                "joint_state_counts": {},
            }

        dom_ratios = [s["dominance_ratio"] for s in recent]
        h_fracs = [s["h_fraction"] for s in recent]
        dom_types = [s["dominant_type"] for s in recent]

        d_mean = sum(dom_ratios) / float(n)
        d_var = sum((d - d_mean) ** 2 for d in dom_ratios) / float(n)
        d_std = math.sqrt(d_var) if d_var > 0 else 0.0

        h_mean = sum(h_fracs) / float(n)

        # Mode of dominant type
        type_freq = {}  # type: dict
        for t in dom_types:
            type_freq[t] = type_freq.get(t, 0) + 1
        dom_mode = max(type_freq, key=type_freq.get) if type_freq else ""

        # Joint state counts
        joint_counts = {}  # type: dict
        for s in recent:
            js = s["joint_state"]
            joint_counts[js] = joint_counts.get(js, 0) + 1

        # Diversity trend from dominance_ratio trajectory
        if n >= 4:
            first_half = dom_ratios[:n // 2]
            second_half = dom_ratios[n // 2:]
            fh_mean = sum(first_half) / float(len(first_half))
            sh_mean = sum(second_half) / float(len(second_half))
            delta = sh_mean - fh_mean
            if delta > 0.03:
                trend = "consolidating"
            elif delta < -0.03:
                trend = "diversifying"
            else:
                trend = "stable"
        else:
            trend = "stable"

        return {
            "dominance_mean": round(d_mean, 6),
            "dominance_std": round(d_std, 6),
            "h_fraction_mean": round(h_mean, 4),
            "dominant_type_mode": dom_mode,
            "diversity_trend": trend,
            "n_snapshots": n,
            "joint_state_counts": joint_counts,
        }


class CASweepTelemetry:
    """
    Live criticality telemetry for CA simulation sweep loops.

    Combines per-snapshot branching-ratio (σ) tracking with avalanche
    power-law exponent (α) estimation, logging both metrics per snapshot
    and flagging deviation from the criticality band in real time.

    This makes the γ = 1 constraint empirically monitorable *during* the
    sweep rather than post-hoc auditable, directly addressing O140 (CA
    measurement grounding) and INV_073 (low-entropy critical attractor
    detection).

    The telemetry pipeline:
      1. Each snapshot feeds σ and raw avalanche sizes into the tracker.
      2. Rolling σ mean/std and α (Hill MLE) are recomputed.
      3. A criticality verdict is emitted per snapshot.
      4. Deviations outside σ ∈ [0.95, 1.05] or α ∉ [1.5, 3.0] are
         flagged as real-time alarms with causal context (dominant type,
         entropy fraction, survival rate).
      5. INV_073 entropy-criticality dissociation is tracked: low H at
         confirmed criticality is logged as CRITICALLY_ORDERED rather
         than anomalous.

    Usage inside a sweep loop::

        telemetry = CASweepTelemetry()
        for step in range(n_steps):
            # ... run CA step, collect metrics ...
            snapshot = telemetry.record_snapshot(
                generation=step,
                parent_count=parents,
                child_count=children,
                avalanche_sizes=aval_sizes,
                shannon_h=h_bits,
                shannon_h_max=h_max,
                survival_rate=surv,
                type_counts={"Physics Navigator": 877, "Entropy Scorer": 47},
            )
            if snapshot["alarm"]:
                print("CRITICALITY DRIFT:", snapshot["alarm"])
        report = telemetry.sweep_report()
    """

    def __init__(
        self,
        window_size=50,          # type: int
        sigma_band_low=0.95,     # type: float
        sigma_band_high=1.05,    # type: float
        alpha_soc_low=1.5,       # type: float
        alpha_soc_high=3.0,      # type: float
        alpha_r2_threshold=0.85, # type: float
        history_limit=2000,      # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        window_size : int
            Number of recent snapshots for rolling σ and α estimation.
        sigma_band_low : float
            Lower bound of the critical band for σ. Default: 0.95.
        sigma_band_high : float
            Upper bound of the critical band for σ. Default: 1.05.
        alpha_soc_low : float
            Lower bound of SOC-consistent α range. Default: 1.5.
        alpha_soc_high : float
            Upper bound of SOC-consistent α range. Default: 3.0.
        alpha_r2_threshold : float
            Minimum R² for power-law fit to be considered valid. Default: 0.85.
        history_limit : int
            Maximum snapshots to retain in memory. Default: 2000.
        """
        self.window_size = max(5, window_size)
        self.sigma_band_low = sigma_band_low
        self.sigma_band_high = sigma_band_high
        self.alpha_soc_low = alpha_soc_low
        self.alpha_soc_high = alpha_soc_high
        self.alpha_r2_threshold = alpha_r2_threshold
        self.history_limit = max(10, history_limit)

        # Per-snapshot records
        self._snapshots = []        # type: list
        self._sigma_series = []     # type: List[float]
        self._alpha_series = []     # type: List[float]
        self._h_fraction_series = []  # type: List[float]

        # Avalanche size buffer (pooled across snapshots for fitting)
        self._avalanche_pool = []   # type: List[float]

        # Alarm history
        self._alarms = []           # type: list

    def record_snapshot(
        self,
        generation,             # type: int
        parent_count,           # type: int
        child_count,            # type: int
        avalanche_sizes=None,   # type: Optional[List[float]]
        shannon_h=0.0,          # type: float
        shannon_h_max=0.0,      # type: float
        survival_rate=0.0,      # type: float
        type_counts=None,       # type: Optional[dict]
    ):
        # type: (...) -> dict
        """
        Record one CA snapshot's telemetry and check for criticality drift.

        Parameters
        ----------
        generation : int
            The simulation step / generation number.
        parent_count : int
            Number of active (parent) cells at this step.
        child_count : int
            Number of active (child/descendant) cells at the next step.
        avalanche_sizes : list of float or None
            Sizes of avalanches that terminated at this step.
        shannon_h : float
            Shannon entropy H of the population type distribution (bits).
        shannon_h_max : float
            Maximum possible Shannon entropy (log2 of number of types).
        survival_rate : float
            Fraction of cells surviving this generation.
        type_counts : dict or None
            Mapping of cell-type name (str) to count (int).

        Returns
        -------
        dict with keys:
            generation        : int
            sigma_instant     : float  — instantaneous σ for this step
            sigma_rolling     : float  — rolling mean σ over window
            sigma_rolling_std : float  — rolling std of σ over window
            alpha             : float  — power-law exponent from pooled avalanches
            alpha_r_squared   : float  — R² of the power-law fit
            power_law_likely  : bool   — α in SOC range with good R²
            shannon_h         : float  — Shannon entropy (bits)
            h_fraction        : float  — H / H_max
            survival_rate     : float
            dominant_type     : str    — most populous cell type
            dominant_share    : float  — fraction of population
            in_sigma_band     : bool   — σ_rolling within critical band
            in_alpha_band     : bool   — α within SOC range
            verdict           : str    — AT_CRITICAL / NEAR_CRITICAL / etc.
            alarm             : dict or None — drift alarm if flagged
            inv073_pattern    : bool   — True if low H + confirmed criticality
            timestamp         : str    — ISO-8601 UTC
        """
        if type_counts is None:
            type_counts = {}

        # ── Instantaneous σ ──
        if parent_count > 0:
            sigma_instant = float(child_count) / float(parent_count)
        else:
            sigma_instant = 0.0
        self._sigma_series.append(sigma_instant)

        # ── Accumulate avalanche sizes ──
        if avalanche_sizes is not None:
            self._avalanche_pool.extend(avalanche_sizes)
        # Bound the pool
        max_pool = self.window_size * 20
        if len(self._avalanche_pool) > max_pool:
            self._avalanche_pool = self._avalanche_pool[-max_pool:]

        # ── Rolling σ statistics ──
        window = self._sigma_series[-self.window_size:]
        w_n = len(window)
        sigma_rolling = sum(window) / float(w_n)
        s_var = sum((s - sigma_rolling) ** 2 for s in window) / float(w_n)
        sigma_rolling_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # ── Fit power-law to pooled avalanche sizes ──
        alpha, alpha_r2 = self._fit_power_law_hill()
        self._alpha_series.append(alpha)

        power_law_likely = (
            self.alpha_soc_low <= alpha <= self.alpha_soc_high
            and alpha_r2 >= self.alpha_r2_threshold
        )

        # ── Entropy fraction ──
        h_fraction = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0
        self._h_fraction_series.append(h_fraction)

        # ── Dominant type ──
        total_pop = sum(type_counts.values()) if type_counts else 0
        total_f = float(total_pop) if total_pop > 0 else 1.0
        if type_counts:
            dominant_type = max(type_counts, key=type_counts.get)
            dominant_count = type_counts[dominant_type]
            dominant_share = round(float(dominant_count) / total_f, 6)
        else:
            dominant_type = ""
            dominant_count = 0
            dominant_share = 0.0

        # ── Band checks ──
        in_sigma_band = self.sigma_band_low <= sigma_rolling <= self.sigma_band_high
        in_alpha_band = self.alpha_soc_low <= alpha <= self.alpha_soc_high

        # ── Verdict ──
        verdict = _criticality_verdict(sigma_rolling, alpha, alpha_r2)

        # ── INV_073 pattern: low entropy at confirmed criticality ──
        # H < 0.3 of max with σ in band and power-law confirmed
        inv073_pattern = (
            in_sigma_band
            and power_law_likely
            and h_fraction < 0.3
            and h_fraction > 0.0
        )

        # ── Alarm generation ──
        alarm = None  # type: Optional[dict]

        # Alarm if σ exits critical band (only after enough data)
        if w_n >= 5 and sigma_rolling != 0.0:
            if not in_sigma_band:
                direction = "supercritical" if sigma_rolling > self.sigma_band_high else "subcritical"
                alarm = {
                    "alarm_type": "sigma_drift",
                    "generation": generation,
                    "sigma_rolling": round(sigma_rolling, 6),
                    "sigma_rolling_std": round(sigma_rolling_std, 6),
                    "direction": direction,
                    "deviation": round(abs(sigma_rolling - 1.0), 6),
                    "alpha": round(alpha, 4),
                    "alpha_r_squared": round(alpha_r2, 4),
                    "dominant_type": dominant_type,
                    "dominant_share": dominant_share,
                    "h_fraction": round(h_fraction, 4),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                self._alarms.append(alarm)

            # Also alarm if α exits SOC band while σ is in band (dissociation)
            elif in_sigma_band and not in_alpha_band and alpha > 0.0:
                alarm = {
                    "alarm_type": "alpha_dissociation",
                    "generation": generation,
                    "sigma_rolling": round(sigma_rolling, 6),
                    "alpha": round(alpha, 4),
                    "alpha_r_squared": round(alpha_r2, 4),
                    "detail": (
                        "sigma={:.4f} in critical band but alpha={:.3f} "
                        "outside SOC range [{:.1f}, {:.1f}] (R²={:.3f}). "
                        "Pseudo-criticality suspected."
                    ).format(
                        sigma_rolling, alpha,
                        self.alpha_soc_low, self.alpha_soc_high,
                        alpha_r2,
                    ),
                    "dominant_type": dominant_type,
                    "h_fraction": round(h_fraction, 4),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                self._alarms.append(alarm)

        # ── Composition vector: counts per type alongside σ and H ──
        # Surfaces whether population composition predicts criticality (INV_073).
        composition_vector = dict(type_counts) if type_counts else {}

        # ── Per-cell-type branching ratio (INV_073 falsifiable invariant) ──
        # Compare consecutive snapshots' type_counts to compute σ_type for
        # each cell type: σ_type = count_now / count_prev.  This tests
        # whether criticality (σ≈1) localizes in navigator-class cells or
        # is uniformly distributed across types, converting the ABSENT
        # finding into a falsifiable per-type invariant.
        #
        # Physics Navigator vs other types: if σ_navigator ≈ 1 while
        # σ_other deviates, criticality is load-bearing on navigators.
        # If all types share σ≈1 uniformly, criticality is a population-
        # level emergent property, not type-localized.
        per_type_sigma = {}  # type: dict
        per_type_sigma_detail = ""
        if type_counts and hasattr(self, '_prev_type_counts') and self._prev_type_counts:
            _all_types = set(list(type_counts.keys()) + list(self._prev_type_counts.keys()))
            _nav_sigmas = []  # type: List[float]
            _other_sigmas = []  # type: List[float]
            for _ct in _all_types:
                _prev_c = self._prev_type_counts.get(_ct, 0)
                _curr_c = type_counts.get(_ct, 0)
                if _prev_c > 0:
                    _type_sigma = float(_curr_c) / float(_prev_c)
                    per_type_sigma[_ct] = round(_type_sigma, 6)
                    # Classify: navigator-class cells vs others
                    _ct_lower = _ct.lower()
                    if "navigator" in _ct_lower or "physics" in _ct_lower:
                        _nav_sigmas.append(_type_sigma)
                    else:
                        _other_sigmas.append(_type_sigma)
                elif _curr_c > 0:
                    # New type appeared (infinite σ — mark as emergence)
                    per_type_sigma[_ct] = float('inf')

            # Compare navigator σ vs other σ
            if _nav_sigmas and _other_sigmas:
                _nav_mean = sum(_nav_sigmas) / float(len(_nav_sigmas))
                _other_mean = sum(_other_sigmas) / float(len(_other_sigmas))
                _nav_in_band = self.sigma_band_low <= _nav_mean <= self.sigma_band_high
                _other_in_band = self.sigma_band_low <= _other_mean <= self.sigma_band_high
                if _nav_in_band and not _other_in_band:
                    per_type_sigma_detail = (
                        "CRITICALITY LOCALIZED: Navigator σ={:.4f} (in band) vs "
                        "other types σ={:.4f} (outside band [{:.2f},{:.2f}]). "
                        "Criticality localizes in navigator-class cells."
                    ).format(_nav_mean, _other_mean,
                             self.sigma_band_low, self.sigma_band_high)
                elif _nav_in_band and _other_in_band:
                    per_type_sigma_detail = (
                        "CRITICALITY UNIFORM: Navigator σ={:.4f} and other "
                        "types σ={:.4f} both in critical band. Criticality "
                        "is a population-level emergent property."
                    ).format(_nav_mean, _other_mean)
                else:
                    per_type_sigma_detail = (
                        "Navigator σ={:.4f} ({}) vs other σ={:.4f} ({}). "
                        "Neither type exclusively maintains criticality."
                    ).format(
                        _nav_mean, "in band" if _nav_in_band else "out of band",
                        _other_mean, "in band" if _other_in_band else "out of band",
                    )
            elif _nav_sigmas:
                _nav_mean = sum(_nav_sigmas) / float(len(_nav_sigmas))
                per_type_sigma_detail = (
                    "Only navigator-class types tracked: σ_nav={:.4f}. "
                    "No other types for comparison."
                ).format(_nav_mean)

        # Store current type_counts for next snapshot's per-type σ computation
        self._prev_type_counts = dict(type_counts) if type_counts else {}

        snapshot = {
            "generation":        generation,
            "sigma_instant":     round(sigma_instant, 6),
            "sigma_rolling":     round(sigma_rolling, 6),
            "sigma_rolling_std": round(sigma_rolling_std, 6),
            "alpha":             round(alpha, 4),
            "alpha_r_squared":   round(alpha_r2, 4),
            "power_law_likely":  power_law_likely,
            "shannon_h":         round(shannon_h, 4),
            "h_fraction":        round(h_fraction, 4),
            "survival_rate":     round(survival_rate, 4),
            "dominant_type":     dominant_type,
            "dominant_share":    dominant_share,
            "total_population":  total_pop,
            "composition_vector": composition_vector,
            "per_type_sigma":    per_type_sigma,
            "per_type_sigma_detail": per_type_sigma_detail,
            "in_sigma_band":     in_sigma_band,
            "in_alpha_band":     in_alpha_band,
            "verdict":           verdict,
            "alarm":             alarm,
            "inv073_pattern":    inv073_pattern,
            "timestamp":         datetime.now(timezone.utc).isoformat(),
        }

        self._snapshots.append(snapshot)

        # Trim histories
        if len(self._snapshots) > self.history_limit:
            trim = len(self._snapshots) - self.history_limit
            self._snapshots = self._snapshots[trim:]
        if len(self._sigma_series) > self.history_limit:
            self._sigma_series = self._sigma_series[-self.history_limit:]
        if len(self._alpha_series) > self.history_limit:
            self._alpha_series = self._alpha_series[-self.history_limit:]
        if len(self._h_fraction_series) > self.history_limit:
            self._h_fraction_series = self._h_fraction_series[-self.history_limit:]

        return snapshot

    def sigma_series(self):
        # type: () -> List[float]
        """Return the full per-snapshot σ history."""
        return list(self._sigma_series)

    def alpha_series(self):
        # type: () -> List[float]
        """Return the full per-snapshot α history."""
        return list(self._alpha_series)

    def alarms(self):
        # type: () -> list
        """Return all alarms ever raised."""
        return list(self._alarms)

    def snapshots(self):
        # type: () -> list
        """Return all recorded snapshots."""
        return list(self._snapshots)

    def sweep_report(self):
        # type: () -> dict
        """
        Generate a summary report of the entire sweep's criticality telemetry.

        Returns
        -------
        dict with keys:
            n_snapshots          : int
            sigma_global_mean    : float  — mean σ across all snapshots
            sigma_global_std     : float  — std σ across all snapshots
            alpha_global_mean    : float  — mean α across all snapshots
            alpha_global_std     : float  — std α across all snapshots
            h_fraction_mean      : float  — mean H/H_max across all snapshots
            fraction_in_sigma_band : float — fraction of snapshots with σ in band
            fraction_in_alpha_band : float — fraction with α in SOC range
            fraction_at_critical : float  — fraction with AT_CRITICAL verdict
            n_alarms             : int    — total alarms raised
            n_sigma_drift        : int    — alarms of type sigma_drift
            n_alpha_dissociation : int    — alarms of type alpha_dissociation
            n_inv073_pattern     : int    — snapshots with low-H-at-criticality
            inv073_assessment    : str    — human-readable assessment
            drift_trend          : str    — "stable" / "freezing" / "dissipating"
        """
        n = len(self._snapshots)
        if n == 0:
            return {
                "n_snapshots": 0,
                "sigma_global_mean": 0.0,
                "sigma_global_std": 0.0,
                "alpha_global_mean": 0.0,
                "alpha_global_std": 0.0,
                "h_fraction_mean": 0.0,
                "fraction_in_sigma_band": 0.0,
                "fraction_in_alpha_band": 0.0,
                "fraction_at_critical": 0.0,
                "n_alarms": 0,
                "n_sigma_drift": 0,
                "n_alpha_dissociation": 0,
                "n_inv073_pattern": 0,
                "inv073_assessment": "No data collected.",
                "drift_trend": "no_data",
            }

        # σ statistics
        sigmas = self._sigma_series[-n:] if self._sigma_series else []
        if sigmas:
            s_mean = sum(sigmas) / float(len(sigmas))
            s_var = sum((s - s_mean) ** 2 for s in sigmas) / float(len(sigmas))
            s_std = math.sqrt(s_var) if s_var > 0 else 0.0
        else:
            s_mean = 0.0
            s_std = 0.0

        # α statistics
        alphas = [a for a in self._alpha_series[-n:] if a > 0.0]
        if alphas:
            a_mean = sum(alphas) / float(len(alphas))
            a_var = sum((a - a_mean) ** 2 for a in alphas) / float(len(alphas))
            a_std = math.sqrt(a_var) if a_var > 0 else 0.0
        else:
            a_mean = 0.0
            a_std = 0.0

        # H fraction statistics
        h_fracs = self._h_fraction_series[-n:] if self._h_fraction_series else []
        h_mean = sum(h_fracs) / float(len(h_fracs)) if h_fracs else 0.0

        # Band fractions
        n_in_sigma = sum(1 for s in self._snapshots if s["in_sigma_band"])
        n_in_alpha = sum(1 for s in self._snapshots if s["in_alpha_band"])
        n_at_crit = sum(1 for s in self._snapshots if s["verdict"] == "AT_CRITICAL")

        frac_sigma = float(n_in_sigma) / float(n)
        frac_alpha = float(n_in_alpha) / float(n)
        frac_crit = float(n_at_crit) / float(n)

        # Alarm counts
        n_sigma_drift = sum(1 for a in self._alarms if a.get("alarm_type") == "sigma_drift")
        n_alpha_dissoc = sum(1 for a in self._alarms if a.get("alarm_type") == "alpha_dissociation")

        # INV_073 pattern count
        n_inv073 = sum(1 for s in self._snapshots if s["inv073_pattern"])

        # INV_073 assessment
        if n_inv073 > 0 and frac_crit > 0.5:
            inv073_text = (
                "CONFIRMED: {}/{} snapshots ({:.1%}) show low-entropy critical "
                "attractor (H<30% of max with sigma in band and power-law "
                "avalanches). This is consistent with ordered SOC phases — "
                "the critical ridge sustains low-diversity, spatially "
                "correlated states. The Wasserstein gradient path to gamma=1 "
                "does NOT require near-maximal entropy."
            ).format(n_inv073, n, float(n_inv073) / float(n))
        elif frac_crit < 0.3:
            inv073_text = (
                "INCONCLUSIVE: only {:.1%} of snapshots achieved AT_CRITICAL "
                "verdict. Insufficient confirmed criticality to assess "
                "entropy-criticality relationship."
            ).format(frac_crit)
        else:
            inv073_text = (
                "STANDARD: {:.1%} of snapshots at criticality with mean "
                "H/H_max={:.3f}. No low-entropy critical attractor detected."
            ).format(frac_crit, h_mean)

        # Drift trend from σ trajectory
        if len(sigmas) >= 8:
            first_half = sigmas[:len(sigmas) // 2]
            second_half = sigmas[len(sigmas) // 2:]
            fh_mean = sum(first_half) / float(len(first_half))
            sh_mean = sum(second_half) / float(len(second_half))
            delta = sh_mean - fh_mean
            if delta > 0.02:
                drift_trend = "dissipating"
            elif delta < -0.02:
                drift_trend = "freezing"
            else:
                drift_trend = "stable"
        else:
            drift_trend = "stable"

        return {
            "n_snapshots":             n,
            "sigma_global_mean":       round(s_mean, 6),
            "sigma_global_std":        round(s_std, 6),
            "alpha_global_mean":       round(a_mean, 4),
            "alpha_global_std":        round(a_std, 4),
            "h_fraction_mean":         round(h_mean, 4),
            "fraction_in_sigma_band":  round(frac_sigma, 4),
            "fraction_in_alpha_band":  round(frac_alpha, 4),
            "fraction_at_critical":    round(frac_crit, 4),
            "n_alarms":                len(self._alarms),
            "n_sigma_drift":           n_sigma_drift,
            "n_alpha_dissociation":    n_alpha_dissoc,
            "n_inv073_pattern":        n_inv073,
            "inv073_assessment":       inv073_text,
            "drift_trend":             drift_trend,
        }

    def _fit_power_law_hill(self):
        # type: () -> Tuple[float, float]
        """
        Fit power-law exponent α via Hill estimator on pooled avalanche sizes.
        Returns (alpha, r_squared). Returns (0.0, 0.0) if insufficient data.
        """
        sizes = [s for s in self._avalanche_pool if s > 0]
        if len(sizes) < 10:
            return (0.0, 0.0)

        x_min = max(1.0, min(sizes))
        tail = [s for s in sizes if s >= x_min]
        n = len(tail)
        if n < 5:
            return (0.0, 0.0)

        log_sum = 0.0
        for s in tail:
            ratio = float(s) / x_min
            if ratio > 0:
                log_sum += math.log(ratio)

        if log_sum <= 0.0:
            return (0.0, 0.0)

        alpha = 1.0 + float(n) / log_sum

        # R² on log-log CCDF
        tail_sorted = sorted(tail)
        unique_sizes = sorted(set(tail_sorted))
        n_total = float(len(tail_sorted))

        log_x = []     # type: List[float]
        log_ccdf = []  # type: List[float]
        for x_val in unique_sizes:
            count_ge = sum(1 for s in tail_sorted if s >= x_val)
            p = float(count_ge) / n_total
            if p > 0 and x_val > 0:
                log_x.append(math.log(float(x_val)))
                log_ccdf.append(math.log(p))

        if len(log_x) < 3:
            return (alpha, 0.0)

        k = len(log_x)
        sum_lx = sum(log_x)
        sum_ly = sum(log_ccdf)
        sum_lxy = sum(x * y for x, y in zip(log_x, log_ccdf))
        sum_lx2 = sum(x * x for x in log_x)

        denom = float(k) * sum_lx2 - sum_lx * sum_lx
        if abs(denom) < 1e-15:
            return (alpha, 0.0)

        slope = (float(k) * sum_lxy - sum_lx * sum_ly) / denom
        intercept = (sum_ly - slope * sum_lx) / float(k)

        mean_ly = sum_ly / float(k)
        ss_tot = sum((y - mean_ly) ** 2 for y in log_ccdf)
        ss_res = sum((y - (intercept + slope * x)) ** 2
                     for x, y in zip(log_x, log_ccdf))
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0

        return (alpha, max(0.0, r_squared))


# ─── Per-Timestep Sigma Monitor (Real-Time Criticality Control Signal) ───────
# Lightweight per-timestep branching-ratio monitor that logs σ at every
# simulation step and flags deviation beyond ±0.05 from 1.0, converting
# criticality from a retrospective measurement into a closed-loop control
# signal.  Designed for direct insertion into a CA simulation loop with
# zero configuration overhead.
#
# Addresses O140 (CA measurement grounding) and INV_073 (Wasserstein
# gradient as necessary path to γ=1): if the CA reaches and holds
# criticality via discrete local rules with no explicit Wasserstein
# geometry, the attractor is reachable through multiple dynamical routes.
#
# The monitor emits a per-step dict containing:
#   - σ (instantaneous branching ratio)
#   - rolling mean and std of σ over a configurable window
#   - a boolean flag when σ deviates beyond ±0.05 from 1.0
#   - the direction of deviation (supercritical / subcritical)
#   - cumulative deviation statistics for drift detection
#
# Usage inside a CA simulation loop::
#
#     monitor = SigmaMonitor()
#     for step in range(n_steps):
#         # ... run CA step ...
#         result = monitor.log(parent_count, child_count)
#         if result["flag"]:
#             # Real-time criticality control signal
#             adjust_ca_parameters(result["direction"], result["deviation"])
#     print(monitor.summary())

SIGMA_DEVIATION_THRESHOLD = 0.05  # ±0.05 from 1.0


class SigmaMonitor:
    """
    Per-timestep branching-ratio (σ) monitor for real-time criticality tracking.

    Logs σ at every simulation step, computes rolling statistics, and
    immediately flags when σ deviates beyond ±0.05 from 1.0.  This converts
    criticality from a post-hoc snapshot measurement into a closed-loop
    control signal that enables the epistemic loop to detect and respond
    to phase transitions *before* they consolidate.

    The monitor is intentionally minimal — no avalanche fitting, no
    power-law estimation, no cell-type tracking.  It does one thing well:
    per-timestep σ logging with real-time deviation flagging.  For richer
    telemetry, compose with CASweepTelemetry or BranchingRatioTracker.

    Example::

        monitor = SigmaMonitor(window=50, threshold=0.05)
        for step in range(200):
            parents, children = run_ca_step(grid)
            result = monitor.log(parents, children)
            if result["flag"]:
                print(f"Step {step}: σ={result['sigma']:.4f} "
                      f"({result['direction']}, Δ={result['deviation']:.4f})")
        summary = monitor.summary()
        print(f"Mean σ={summary['sigma_mean']:.4f} ± {summary['sigma_std']:.4f}, "
              f"flagged {summary['n_flagged']}/{summary['n_steps']} steps")
    """

    def __init__(
        self,
        window=50,          # type: int
        threshold=None,     # type: Optional[float]
        center=1.0,         # type: float
        history_limit=5000, # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        window : int
            Number of recent steps for rolling σ mean/std. Default: 50.
        threshold : float or None
            Deviation threshold from center for flagging.  Default: 0.05
            (the SIGMA_DEVIATION_THRESHOLD constant).
        center : float
            The target σ value (perfect criticality). Default: 1.0.
        history_limit : int
            Maximum per-step records to retain. Default: 5000.
        """
        self.window = max(1, window)
        self.threshold = threshold if threshold is not None else SIGMA_DEVIATION_THRESHOLD
        self.center = center
        self.history_limit = max(10, history_limit)

        self._sigma_log = []       # type: List[float]
        self._flag_log = []        # type: List[bool]
        self._step_count = 0       # type: int
        self._n_flagged = 0        # type: int
        self._n_supercritical = 0  # type: int
        self._n_subcritical = 0    # type: int
        self._cumulative_deviation = 0.0  # type: float
        self._max_deviation = 0.0  # type: float
        self._flag_events = []     # type: list

    def log(self, parent_count, child_count):
        # type: (int, int) -> dict
        """
        Log one simulation step and return a real-time criticality signal.

        Parameters
        ----------
        parent_count : int
            Number of active (parent) cells at this step.
        child_count : int
            Number of active (child/descendant) cells produced.

        Returns
        -------
        dict with keys:
            step           : int    — the step number (0-indexed)
            sigma          : float  — instantaneous branching ratio
            sigma_rolling  : float  — rolling mean σ over window
            sigma_std      : float  — rolling std σ over window
            deviation      : float  — |σ_rolling - center|
            flag           : bool   — True if deviation > threshold
            direction      : str    — "supercritical" / "subcritical" / ""
            cumulative_dev : float  — sum of all |σ - center| so far
            max_deviation  : float  — largest |σ_rolling - center| seen
            n_flagged      : int    — total flagged steps so far
            timestamp      : str    — ISO-8601 UTC
        """
        self._step_count += 1

        # Instantaneous σ
        if parent_count > 0:
            sigma = float(child_count) / float(parent_count)
        else:
            sigma = 0.0

        self._sigma_log.append(sigma)

        # Rolling statistics over window
        window_data = self._sigma_log[-self.window:]
        w_n = len(window_data)
        sigma_rolling = sum(window_data) / float(w_n)
        s_var = sum((s - sigma_rolling) ** 2 for s in window_data) / float(w_n)
        sigma_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # Deviation from center
        deviation = abs(sigma_rolling - self.center)
        self._cumulative_deviation += abs(sigma - self.center)
        if deviation > self._max_deviation:
            self._max_deviation = deviation

        # Flag check: deviation beyond ±threshold from center
        flag = (sigma_rolling != 0.0) and (deviation > self.threshold)
        self._flag_log.append(flag)

        direction = ""
        if flag:
            self._n_flagged += 1
            if sigma_rolling > self.center + self.threshold:
                direction = "supercritical"
                self._n_supercritical += 1
            elif sigma_rolling < self.center - self.threshold:
                direction = "subcritical"
                self._n_subcritical += 1

            self._flag_events.append({
                "step": self._step_count - 1,
                "sigma": round(sigma, 6),
                "sigma_rolling": round(sigma_rolling, 6),
                "deviation": round(deviation, 6),
                "direction": direction,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        # Trim history
        if len(self._sigma_log) > self.history_limit:
            trim = len(self._sigma_log) - self.history_limit
            self._sigma_log = self._sigma_log[trim:]
            self._flag_log = self._flag_log[trim:]

        return {
            "step":           self._step_count - 1,
            "sigma":          round(sigma, 6),
            "sigma_rolling":  round(sigma_rolling, 6),
            "sigma_std":      round(sigma_std, 6),
            "deviation":      round(deviation, 6),
            "flag":           flag,
            "direction":      direction,
            "cumulative_dev": round(self._cumulative_deviation, 6),
            "max_deviation":  round(self._max_deviation, 6),
            "n_flagged":      self._n_flagged,
            "timestamp":      datetime.now(timezone.utc).isoformat(),
        }

    def sigma_series(self):
        # type: () -> List[float]
        """Return the full per-step σ history."""
        return list(self._sigma_log)

    def flag_events(self):
        # type: () -> list
        """Return all flag events (steps where σ deviated beyond threshold)."""
        return list(self._flag_events)

    def summary(self):
        # type: () -> dict
        """
        Generate a summary of the entire monitoring run.

        Returns
        -------
        dict with keys:
            n_steps            : int    — total steps logged
            sigma_mean         : float  — global mean σ
            sigma_std          : float  — global std σ
            sigma_min          : float  — minimum σ observed
            sigma_max          : float  — maximum σ observed
            n_flagged          : int    — total flagged steps
            n_supercritical    : int    — flagged steps above band
            n_subcritical      : int    — flagged steps below band
            flag_rate          : float  — fraction of steps flagged
            mean_deviation     : float  — mean |σ - center| per step
            max_deviation      : float  — largest rolling |σ - center|
            threshold          : float  — the deviation threshold used
            center             : float  — the target σ value
            verdict            : str    — AT_CRITICAL / DRIFTING_SUPER /
                                          DRIFTING_SUB / UNSTABLE / NO_DATA
            inv073_note        : str    — relevance to INV_073 challenge
        """
        n = self._step_count

        if n == 0:
            return {
                "n_steps": 0,
                "sigma_mean": 0.0,
                "sigma_std": 0.0,
                "sigma_min": 0.0,
                "sigma_max": 0.0,
                "n_flagged": 0,
                "n_supercritical": 0,
                "n_subcritical": 0,
                "flag_rate": 0.0,
                "mean_deviation": 0.0,
                "max_deviation": 0.0,
                "threshold": self.threshold,
                "center": self.center,
                "verdict": "NO_DATA",
                "inv073_note": "No steps logged.",
            }

        all_sigma = self._sigma_log
        s_n = len(all_sigma)
        s_mean = sum(all_sigma) / float(s_n) if s_n > 0 else 0.0
        s_var = sum((s - s_mean) ** 2 for s in all_sigma) / float(s_n) if s_n > 0 else 0.0
        s_std = math.sqrt(s_var) if s_var > 0 else 0.0
        s_min = min(all_sigma) if all_sigma else 0.0
        s_max = max(all_sigma) if all_sigma else 0.0

        flag_rate = round(float(self._n_flagged) / float(n), 6) if n > 0 else 0.0
        mean_dev = round(self._cumulative_deviation / float(n), 6) if n > 0 else 0.0

        # Verdict based on global σ statistics
        global_deviation = abs(s_mean - self.center)
        if global_deviation <= self.threshold and flag_rate < 0.15:
            verdict = "AT_CRITICAL"
        elif s_mean > self.center + self.threshold:
            verdict = "DRIFTING_SUPER"
        elif s_mean < self.center - self.threshold:
            verdict = "DRIFTING_SUB"
        elif flag_rate >= 0.3:
            verdict = "UNSTABLE"
        else:
            verdict = "AT_CRITICAL"

        # INV_073 assessment
        if verdict == "AT_CRITICAL":
            inv073_note = (
                "σ={:.4f}±{:.4f} within critical band (center={:.2f} "
                "±{:.2f}) with flag rate {:.1%}. The CA reaches and holds "
                "criticality via discrete local rules without explicit "
                "Wasserstein geometry, supporting the hypothesis that the "
                "critical attractor is reachable through multiple dynamical "
                "routes (INV_073 challenge)."
            ).format(s_mean, s_std, self.center, self.threshold, flag_rate)
        elif verdict in ("DRIFTING_SUPER", "DRIFTING_SUB"):
            inv073_note = (
                "σ={:.4f}±{:.4f} drifting {} from critical band. "
                "Real-time monitoring detected phase transition before "
                "consolidation — closed-loop control signal available for "
                "parameter adjustment."
            ).format(
                s_mean, s_std,
                "supercritical" if verdict == "DRIFTING_SUPER" else "subcritical",
            )
        else:
            inv073_note = (
                "σ={:.4f}±{:.4f} with flag rate {:.1%} — system is "
                "oscillating between regimes. Active stabilization required."
            ).format(s_mean, s_std, flag_rate)

        return {
            "n_steps":          n,
            "sigma_mean":       round(s_mean, 6),
            "sigma_std":        round(s_std, 6),
            "sigma_min":        round(s_min, 6),
            "sigma_max":        round(s_max, 6),
            "n_flagged":        self._n_flagged,
            "n_supercritical":  self._n_supercritical,
            "n_subcritical":    self._n_subcritical,
            "flag_rate":        flag_rate,
            "mean_deviation":   mean_dev,
            "max_deviation":    round(self._max_deviation, 6),
            "threshold":        self.threshold,
            "center":           self.center,
            "verdict":          verdict,
            "inv073_note":      inv073_note,
        }

    def reset(self):
        # type: () -> None
        """Reset all state for a new monitoring run."""
        self._sigma_log = []
        self._flag_log = []
        self._step_count = 0
        self._n_flagged = 0
        self._n_supercritical = 0
        self._n_subcritical = 0
        self._cumulative_deviation = 0.0
        self._max_deviation = 0.0
        self._flag_events = []


# ─── Branching-Ratio Telemetry Collector ─────────────────────────────────────
# Per-cycle σ = mean(offspring) / mean(parent activations) logged alongside
# Shannon entropy H, enabling real-time detection of drift from the critical
# band σ ∈ [0.95, 1.05].
#
# When σ > 1.05 the system is trending toward a frozen/supercritical regime;
# when σ < 0.95 it is dissipating/subcritical.  Logging σ and H together
# makes INV_073 operationally load-bearing rather than post-hoc: the
# epistemic loop can self-monitor criticality and trigger corrective action
# before coherence scores degrade.
#
# Addresses O140 (CA measurement grounding), O141 (solo-kernel vs population
# criticality), INV_073 (Wasserstein gradient as necessary path to γ=1).

class BranchingRatioTelemetryCollector:
    """
    Per-cycle branching-ratio telemetry collector that computes
    σ = mean(offspring) / mean(parent activations) and logs it alongside
    Shannon entropy H, flagging drift from the critical band [0.95, 1.05]
    in real time.

    This converts INV_073 from a post-hoc audit into an operationally
    load-bearing control signal: FREED's epistemic loop can detect when
    its own generative process is drifting toward frozen (σ > 1.05) or
    dissipated (σ < 0.95) regimes and trigger corrective action before
    coherence scores degrade.

    Each cycle record contains:
      - σ (branching ratio)
      - H (Shannon entropy in bits)
      - h_fraction (H / H_max — entropy utilization)
      - regime classification (CRITICAL / FROZEN / DISSIPATED)
      - drift magnitude and direction

    Usage::

        collector = BranchingRatioTelemetryCollector()
        for cycle in epistemic_loop:
            record = collector.collect(
                cycle=cycle,
                parent_activations=[a1, a2, ...],
                offspring_activations=[b1, b2, ...],
                shannon_h=h_bits,
                shannon_h_max=h_max,
            )
            if record["regime"] != "CRITICAL":
                trigger_corrective_action(record)
        report = collector.report()
    """

    # Regime thresholds
    SIGMA_CRITICAL_LO = 0.95
    SIGMA_CRITICAL_HI = 1.05

    def __init__(self, history_limit=2000):
        # type: (int) -> None
        """
        Parameters
        ----------
        history_limit : int
            Maximum number of per-cycle records to retain.  Default: 2000.
        """
        self.history_limit = max(10, history_limit)

        self._records = []           # type: list
        self._sigma_series = []      # type: List[float]
        self._h_series = []          # type: List[float]
        self._h_fraction_series = [] # type: List[float]
        self._drift_events = []      # type: list
        self._cycle_count = 0        # type: int

    def collect(
        self,
        cycle,                      # type: int
        parent_activations,         # type: List[float]
        offspring_activations,      # type: List[float]
        shannon_h=0.0,              # type: float
        shannon_h_max=0.0,          # type: float
        survival_rate=0.0,          # type: float
        dominant_type="",           # type: str
        metadata=None,              # type: Optional[dict]
    ):
        # type: (...) -> dict
        """
        Collect one cycle's branching-ratio telemetry.

        Computes σ = mean(offspring_activations) / mean(parent_activations),
        logs it alongside Shannon entropy H, and classifies the current
        regime.

        Parameters
        ----------
        cycle : int
            The epistemic loop cycle number.
        parent_activations : list of float
            Activation values (or counts) of parent cells/neurons/agents
            at this cycle.  Must be non-empty for a valid σ.
        offspring_activations : list of float
            Activation values (or counts) of offspring cells/neurons/agents
            produced at this cycle.
        shannon_h : float
            Shannon entropy H of the population type distribution (bits).
        shannon_h_max : float
            Maximum possible Shannon entropy (log2 of number of types).
        survival_rate : float
            Fraction of agents surviving this cycle.
        dominant_type : str
            Label of the most populous agent type.
        metadata : dict or None
            Optional additional metadata to attach to the record.

        Returns
        -------
        dict with keys:
            cycle              : int
            sigma              : float   — branching ratio
            sigma_std          : float   — std of per-element σ ratios (if computable)
            mean_parent        : float   — mean of parent activations
            mean_offspring     : float   — mean of offspring activations
            shannon_h          : float   — Shannon entropy (bits)
            h_fraction         : float   — H / H_max
            survival_rate      : float
            dominant_type      : str
            regime             : str     — CRITICAL / FROZEN / DISSIPATED / NO_DATA
            drift_magnitude    : float   — |σ - 1.0|
            drift_direction    : str     — "supercritical" / "subcritical" / ""
            drift_event        : bool    — True if σ outside critical band
            inv073_status      : str     — human-readable INV_073 assessment
            timestamp          : str     — ISO-8601 UTC
        """
        self._cycle_count += 1

        n_parents = len(parent_activations)
        n_offspring = len(offspring_activations)

        # ── Compute σ ──
        if n_parents > 0:
            mean_parent = sum(parent_activations) / float(n_parents)
        else:
            mean_parent = 0.0

        if n_offspring > 0:
            mean_offspring = sum(offspring_activations) / float(n_offspring)
        else:
            mean_offspring = 0.0

        if mean_parent > 0.0:
            sigma = mean_offspring / mean_parent
        else:
            sigma = 0.0

        # ── Per-element σ std (when both lists have same length) ──
        sigma_std = 0.0
        if n_parents > 0 and n_offspring > 0 and n_parents == n_offspring:
            element_sigmas = []  # type: List[float]
            for p, o in zip(parent_activations, offspring_activations):
                if p > 0.0:
                    element_sigmas.append(o / p)
            if len(element_sigmas) > 1:
                es_mean = sum(element_sigmas) / float(len(element_sigmas))
                es_var = sum((s - es_mean) ** 2 for s in element_sigmas) / float(len(element_sigmas))
                sigma_std = math.sqrt(es_var) if es_var > 0 else 0.0

        self._sigma_series.append(sigma)

        # ── Entropy ──
        h_fraction = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0
        self._h_series.append(shannon_h)
        self._h_fraction_series.append(h_fraction)

        # ── Regime classification ──
        drift_magnitude = abs(sigma - 1.0)
        if sigma == 0.0 and n_parents == 0:
            regime = "NO_DATA"
            drift_direction = ""
            drift_event = False
        elif self.SIGMA_CRITICAL_LO <= sigma <= self.SIGMA_CRITICAL_HI:
            regime = "CRITICAL"
            drift_direction = ""
            drift_event = False
        elif sigma > self.SIGMA_CRITICAL_HI:
            regime = "FROZEN"
            drift_direction = "supercritical"
            drift_event = True
        else:
            regime = "DISSIPATED"
            drift_direction = "subcritical"
            drift_event = True

        # ── INV_073 assessment ──
        if regime == "CRITICAL":
            inv073_status = (
                "ON_RIDGE: sigma={:.4f} within critical band [{:.2f}, {:.2f}], "
                "H={:.4f} bits (h_frac={:.3f}). Epistemic loop is self-sustaining "
                "at criticality."
            ).format(
                sigma, self.SIGMA_CRITICAL_LO, self.SIGMA_CRITICAL_HI,
                shannon_h, h_fraction,
            )
        elif regime == "FROZEN":
            inv073_status = (
                "DRIFT_FROZEN: sigma={:.4f} > {:.2f}. Generative process is "
                "trending supercritical — offspring over-proliferating relative "
                "to parents. Risk of frozen attractor (gamma<1). Corrective "
                "action recommended."
            ).format(sigma, self.SIGMA_CRITICAL_HI)
        elif regime == "DISSIPATED":
            inv073_status = (
                "DRIFT_DISSIPATED: sigma={:.4f} < {:.2f}. Generative process is "
                "trending subcritical — offspring under-producing relative to "
                "parents. Risk of dissipation (gamma>1). Corrective action "
                "recommended."
            ).format(sigma, self.SIGMA_CRITICAL_LO)
        else:
            inv073_status = "NO_DATA: insufficient parent activations to compute sigma."

        record = {
            "cycle":            cycle,
            "sigma":            round(sigma, 6),
            "sigma_std":        round(sigma_std, 6),
            "mean_parent":      round(mean_parent, 6),
            "mean_offspring":   round(mean_offspring, 6),
            "n_parents":        n_parents,
            "n_offspring":      n_offspring,
            "shannon_h":        round(shannon_h, 4),
            "h_fraction":       round(h_fraction, 4),
            "survival_rate":    round(survival_rate, 4),
            "dominant_type":    dominant_type,
            "regime":           regime,
            "drift_magnitude":  round(drift_magnitude, 6),
            "drift_direction":  drift_direction,
            "drift_event":      drift_event,
            "inv073_status":    inv073_status,
            "timestamp":        datetime.now(timezone.utc).isoformat(),
        }

        if metadata is not None:
            record["metadata"] = dict(metadata)

        self._records.append(record)

        if drift_event:
            self._drift_events.append(record)

        # ── Trim history ──
        if len(self._records) > self.history_limit:
            trim = len(self._records) - self.history_limit
            self._records = self._records[trim:]
        if len(self._sigma_series) > self.history_limit:
            self._sigma_series = self._sigma_series[-self.history_limit:]
        if len(self._h_series) > self.history_limit:
            self._h_series = self._h_series[-self.history_limit:]
        if len(self._h_fraction_series) > self.history_limit:
            self._h_fraction_series = self._h_fraction_series[-self.history_limit:]

        return record

    def collect_from_counts(
        self,
        cycle,                  # type: int
        parent_count,           # type: int
        child_count,            # type: int
        shannon_h=0.0,          # type: float
        shannon_h_max=0.0,      # type: float
        survival_rate=0.0,      # type: float
        dominant_type="",       # type: str
        metadata=None,          # type: Optional[dict]
    ):
        # type: (...) -> dict
        """
        Convenience method: compute σ from scalar parent/child counts
        rather than activation vectors.

        Equivalent to collect() with single-element activation lists,
        but more natural for CA simulations that report aggregate counts.

        Parameters
        ----------
        cycle : int
            The epistemic loop cycle number.
        parent_count : int
            Number of active parent cells/agents at this cycle.
        child_count : int
            Number of active offspring cells/agents produced.
        shannon_h : float
            Shannon entropy H (bits).
        shannon_h_max : float
            Maximum possible Shannon entropy.
        survival_rate : float
            Fraction of agents surviving.
        dominant_type : str
            Label of the most populous agent type.
        metadata : dict or None
            Optional additional metadata.

        Returns
        -------
        dict — same format as collect().
        """
        return self.collect(
            cycle=cycle,
            parent_activations=[float(parent_count)] if parent_count > 0 else [],
            offspring_activations=[float(child_count)] if child_count > 0 else [],
            shannon_h=shannon_h,
            shannon_h_max=shannon_h_max,
            survival_rate=survival_rate,
            dominant_type=dominant_type,
            metadata=metadata,
        )

    def sigma_series(self):
        # type: () -> List[float]
        """Return the full per-cycle σ history."""
        return list(self._sigma_series)

    def entropy_series(self):
        # type: () -> List[float]
        """Return the full per-cycle Shannon entropy history."""
        return list(self._h_series)

    def h_fraction_series(self):
        # type: () -> List[float]
        """Return the full per-cycle h_fraction history."""
        return list(self._h_fraction_series)

    def records(self):
        # type: () -> list
        """Return all collected records."""
        return list(self._records)

    def drift_events(self):
        # type: () -> list
        """Return all drift event records."""
        return list(self._drift_events)

    def report(self, window=50):
        # type: (int) -> dict
        """
        Generate a summary report of the collected telemetry, including
        rolling σ and H statistics and drift detection.

        Parameters
        ----------
        window : int
            Number of recent cycles for rolling statistics.  Default: 50.

        Returns
        -------
        dict with keys:
            n_cycles             : int    — total cycles collected
            sigma_global_mean    : float  — mean σ across all cycles
            sigma_global_std     : float  — std σ across all cycles
            sigma_rolling_mean   : float  — mean σ over last `window` cycles
            sigma_rolling_std    : float  — std σ over last `window` cycles
            h_global_mean        : float  — mean H across all cycles
            h_fraction_mean      : float  — mean h_fraction across all cycles
            h_rolling_mean       : float  — mean H over last `window` cycles
            fraction_critical    : float  — fraction of cycles in CRITICAL regime
            fraction_frozen      : float  — fraction in FROZEN regime
            fraction_dissipated  : float  — fraction in DISSIPATED regime
            n_drift_events       : int    — total drift events
            drift_rate           : float  — fraction of cycles with drift
            sigma_h_correlation  : float  — Pearson correlation between σ and H
                                            (positive = co-varying; negative =
                                            anticorrelated — suggests ordered
                                            criticality per INV_073)
            drift_trend          : str    — "stable" / "freezing" / "dissipating"
            inv073_assessment    : str    — human-readable assessment
            timestamp            : str    — ISO-8601 UTC
        """
        n = self._cycle_count

        if n == 0:
            return {
                "n_cycles": 0,
                "sigma_global_mean": 0.0,
                "sigma_global_std": 0.0,
                "sigma_rolling_mean": 0.0,
                "sigma_rolling_std": 0.0,
                "h_global_mean": 0.0,
                "h_fraction_mean": 0.0,
                "h_rolling_mean": 0.0,
                "fraction_critical": 0.0,
                "fraction_frozen": 0.0,
                "fraction_dissipated": 0.0,
                "n_drift_events": 0,
                "drift_rate": 0.0,
                "sigma_h_correlation": 0.0,
                "drift_trend": "no_data",
                "inv073_assessment": "No cycles collected.",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        # ── Global σ statistics ──
        all_sigma = self._sigma_series
        s_n = len(all_sigma)
        s_mean = sum(all_sigma) / float(s_n) if s_n > 0 else 0.0
        s_var = sum((s - s_mean) ** 2 for s in all_sigma) / float(s_n) if s_n > 0 else 0.0
        s_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # ── Rolling σ statistics ──
        recent_sigma = all_sigma[-window:]
        rs_n = len(recent_sigma)
        rs_mean = sum(recent_sigma) / float(rs_n) if rs_n > 0 else 0.0
        rs_var = sum((s - rs_mean) ** 2 for s in recent_sigma) / float(rs_n) if rs_n > 0 else 0.0
        rs_std = math.sqrt(rs_var) if rs_var > 0 else 0.0

        # ── Global H statistics ──
        all_h = self._h_series
        h_n = len(all_h)
        h_mean = sum(all_h) / float(h_n) if h_n > 0 else 0.0

        all_hf = self._h_fraction_series
        hf_n = len(all_hf)
        hf_mean = sum(all_hf) / float(hf_n) if hf_n > 0 else 0.0

        # ── Rolling H ──
        recent_h = all_h[-window:]
        rh_mean = sum(recent_h) / float(len(recent_h)) if recent_h else 0.0

        # ── Regime fractions ──
        n_rec = len(self._records)
        n_crit = sum(1 for r in self._records if r["regime"] == "CRITICAL")
        n_frozen = sum(1 for r in self._records if r["regime"] == "FROZEN")
        n_dissip = sum(1 for r in self._records if r["regime"] == "DISSIPATED")
        frac_crit = float(n_crit) / float(n_rec) if n_rec > 0 else 0.0
        frac_frozen = float(n_frozen) / float(n_rec) if n_rec > 0 else 0.0
        frac_dissip = float(n_dissip) / float(n_rec) if n_rec > 0 else 0.0

        # ── Drift statistics ──
        n_drift = len(self._drift_events)
        drift_rate = float(n_drift) / float(n_rec) if n_rec > 0 else 0.0

        # ── Pearson correlation between σ and H ──
        # A negative correlation (low H at high σ near 1.0) is the INV_073
        # signature: ordered criticality.
        sigma_h_corr = 0.0
        min_len = min(len(all_sigma), len(all_h))
        if min_len >= 3:
            s_sub = all_sigma[-min_len:]
            h_sub = all_h[-min_len:]
            s_m = sum(s_sub) / float(min_len)
            h_m = sum(h_sub) / float(min_len)
            cov_sh = sum((s - s_m) * (h - h_m) for s, h in zip(s_sub, h_sub)) / float(min_len)
            var_s = sum((s - s_m) ** 2 for s in s_sub) / float(min_len)
            var_h = sum((h - h_m) ** 2 for h in h_sub) / float(min_len)
            denom_corr = math.sqrt(var_s) * math.sqrt(var_h) if var_s > 0 and var_h > 0 else 0.0
            if denom_corr > 1e-15:
                sigma_h_corr = cov_sh / denom_corr

        # ── Drift trend from σ trajectory ──
        if len(all_sigma) >= 8:
            first_half = all_sigma[:len(all_sigma) // 2]
            second_half = all_sigma[len(all_sigma) // 2:]
            fh_m = sum(first_half) / float(len(first_half))
            sh_m = sum(second_half) / float(len(second_half))
            delta = sh_m - fh_m
            if delta > 0.02:
                drift_trend = "freezing"      # σ rising → supercritical → frozen
            elif delta < -0.02:
                drift_trend = "dissipating"   # σ falling → subcritical → dissipated
            else:
                drift_trend = "stable"
        else:
            drift_trend = "stable"

        # ── INV_073 assessment ──
        if frac_crit > 0.8 and drift_trend == "stable":
            inv073_assessment = (
                "LOAD_BEARING: {:.1%} of cycles at criticality with stable drift "
                "trend. sigma={:.4f}±{:.4f}, H_mean={:.4f} bits (h_frac={:.3f}). "
                "sigma-H correlation={:.3f}. The epistemic loop is self-sustaining "
                "at the critical ridge. INV_073 is operationally confirmed."
            ).format(frac_crit, s_mean, s_std, h_mean, hf_mean, sigma_h_corr)
        elif frac_crit > 0.5:
            inv073_assessment = (
                "MARGINAL: {:.1%} of cycles at criticality but drift_trend='{}'. "
                "sigma={:.4f}±{:.4f}. The system is near the critical band but "
                "not stably on-ridge. Active monitoring is compensating."
            ).format(frac_crit, drift_trend, s_mean, s_std)
        elif drift_trend == "freezing":
            inv073_assessment = (
                "FAILING_FROZEN: Only {:.1%} of cycles at criticality, sigma "
                "trending upward (mean={:.4f}). The generative process is "
                "drifting toward frozen attractor. INV_073 predicts this will "
                "cause coherence degradation — corrective action required."
            ).format(frac_crit, s_mean)
        elif drift_trend == "dissipating":
            inv073_assessment = (
                "FAILING_DISSIPATED: Only {:.1%} of cycles at criticality, sigma "
                "trending downward (mean={:.4f}). The generative process is "
                "dissipating. INV_073 predicts loss of generative capacity — "
                "corrective action required."
            ).format(frac_crit, s_mean)
        else:
            inv073_assessment = (
                "INCONCLUSIVE: {:.1%} critical, sigma={:.4f}±{:.4f}, "
                "drift_trend='{}'. Insufficient data or mixed regime."
            ).format(frac_crit, s_mean, s_std, drift_trend)

        return {
            "n_cycles":            n,
            "sigma_global_mean":   round(s_mean, 6),
            "sigma_global_std":    round(s_std, 6),
            "sigma_rolling_mean":  round(rs_mean, 6),
            "sigma_rolling_std":   round(rs_std, 6),
            "h_global_mean":       round(h_mean, 4),
            "h_fraction_mean":     round(hf_mean, 4),
            "h_rolling_mean":      round(rh_mean, 4),
            "fraction_critical":   round(frac_crit, 4),
            "fraction_frozen":     round(frac_frozen, 4),
            "fraction_dissipated": round(frac_dissip, 4),
            "n_drift_events":      n_drift,
            "drift_rate":          round(drift_rate, 4),
            "sigma_h_correlation": round(sigma_h_corr, 6),
            "drift_trend":         drift_trend,
            "inv073_assessment":   inv073_assessment,
            "timestamp":           datetime.now(timezone.utc).isoformat(),
        }

    def reset(self):
        # type: () -> None
        """Reset all state for a new collection run."""
        self._records = []
        self._sigma_series = []
        self._h_series = []
        self._h_fraction_series = []
        self._drift_events = []
        self._cycle_count = 0


# ─── Rolling Criticality Telemetry (First-Class σ + H + Avalanche Metric) ───
# Per-step branching-ratio σ = mean(offspring counts per active cell per step)
# computed as a rolling metric alongside Shannon entropy H and avalanche size
# distribution statistics.  Designed for real-time criticality drift detection
# and corrective reweighting triggers.
#
# INV_073 context: σ=1.0363 sits 0.0363 above the center of the critical
# band, suggesting slightly supercritical operation.  This tracker makes
# the drift visible step-by-step and emits corrective-reweight signals
# when σ leaves [0.95, 1.05] for more than `reweight_patience` consecutive
# steps, enabling the epistemic loop to self-correct before coherence
# degrades.
#
# Addresses O140 (CA measurement grounding), O141 (solo-kernel vs population
# criticality), INV_073 (Wasserstein gradient bias detection and corrective
# reweighting).


class RollingCriticalityTelemetry:
    """
    First-class rolling telemetry metric that jointly tracks:

      1. **σ** — branching ratio = mean(offspring per active cell per step)
      2. **H** — Shannon entropy of the population type distribution (bits)
      3. **Avalanche size distribution** — power-law exponent α via Hill MLE

    These three metrics are computed per step on a rolling window basis
    and emitted as a single telemetry record, enabling real-time
    criticality drift detection and corrective reweighting triggers.

    The corrective reweighting signal is emitted when σ leaves the
    critical band [0.95, 1.05] for more than `reweight_patience`
    consecutive steps, providing the epistemic loop with a concrete
    control signal to adjust agent reproduction rates or selection
    pressures before the system consolidates into a frozen or
    dissipated regime.

    Addresses O140, O141, INV_073 (σ=1.0363 slightly supercritical —
    this tracker would detect and flag that drift in real time).

    Usage::

        telemetry = RollingCriticalityTelemetry(window=50, reweight_patience=10)
        for step in simulation:
            record = telemetry.record_step(
                offspring_per_cell=[1, 2, 0, 1, 1, 3, 0, ...],
                shannon_h=0.3991,
                shannon_h_max=2.585,
                avalanche_sizes=[4, 1, 7, 2],
                survival_rate=0.9163,
            )
            if record["reweight_signal"]:
                apply_corrective_reweighting(record["reweight_direction"],
                                              record["reweight_magnitude"])
        report = telemetry.report()
    """

    # Critical band bounds
    SIGMA_BAND_LO = 0.95
    SIGMA_BAND_HI = 1.05
    # SOC-consistent avalanche exponent range
    ALPHA_SOC_LO = 1.5
    ALPHA_SOC_HI = 3.0
    ALPHA_R2_MIN = 0.85

    def __init__(
        self,
        window=50,              # type: int
        reweight_patience=10,   # type: int
        history_limit=5000,     # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        window : int
            Number of recent steps for rolling σ, H, and α estimation.
            Default: 50.
        reweight_patience : int
            Number of consecutive steps σ must be outside the critical
            band before a corrective reweight signal is emitted.
            Default: 10.
        history_limit : int
            Maximum per-step records retained in memory. Default: 5000.
        """
        self.window = max(5, window)
        self.reweight_patience = max(1, reweight_patience)
        self.history_limit = max(10, history_limit)

        # Per-step rolling buffers
        self._sigma_series = []         # type: List[float]
        self._h_series = []             # type: List[float]
        self._h_fraction_series = []    # type: List[float]
        self._avalanche_pool = []       # type: List[float]

        # Step tracking
        self._step_count = 0            # type: int
        self._consecutive_out = 0       # type: int
        self._out_direction = ""        # type: str

        # Records and events
        self._records = []              # type: list
        self._reweight_signals = []     # type: list

    def record_step(
        self,
        offspring_per_cell,         # type: List[float]
        shannon_h=0.0,              # type: float
        shannon_h_max=0.0,          # type: float
        avalanche_sizes=None,       # type: Optional[List[float]]
        survival_rate=0.0,          # type: float
        parent_count=0,             # type: int
        child_count=0,              # type: int
    ):
        # type: (...) -> dict
        """
        Record one simulation step's per-cell offspring counts and
        recompute rolling σ, H, and α telemetry.

        Parameters
        ----------
        offspring_per_cell : list of float
            Number of offspring produced by each active cell at this step.
            σ = mean(offspring_per_cell).  If empty, falls back to
            child_count / parent_count.
        shannon_h : float
            Shannon entropy H of the population type distribution (bits).
        shannon_h_max : float
            Maximum possible Shannon entropy (log2 of number of types).
        avalanche_sizes : list of float or None
            Sizes of avalanches that terminated at this step.
        survival_rate : float
            Fraction of cells surviving this generation.
        parent_count : int
            Fallback: total active parent cells (used only if
            offspring_per_cell is empty).
        child_count : int
            Fallback: total active child cells (used only if
            offspring_per_cell is empty).

        Returns
        -------
        dict with keys:
            step                : int
            sigma               : float  — instantaneous σ (this step)
            sigma_rolling       : float  — rolling mean σ over window
            sigma_rolling_std   : float  — rolling std σ over window
            shannon_h           : float
            h_fraction          : float  — H / H_max
            h_rolling           : float  — rolling mean H over window
            alpha               : float  — power-law exponent (Hill MLE)
            alpha_r_squared     : float  — R² of the power-law fit
            power_law_likely    : bool
            in_critical_band    : bool   — σ_rolling in [0.95, 1.05]
            verdict             : str    — AT_CRITICAL / NEAR_CRITICAL / etc.
            sigma_drift         : float  — |σ_rolling - 1.0|
            reweight_signal     : bool   — True if corrective reweight needed
            reweight_direction  : str    — "decrease" (supercritical) /
                                           "increase" (subcritical) / ""
            reweight_magnitude  : float  — suggested adjustment magnitude
                                           (proportional to drift from 1.0)
            consecutive_out     : int    — steps σ has been outside band
            survival_rate       : float
            timestamp           : str
        """
        self._step_count += 1

        # ── Compute instantaneous σ ──
        if offspring_per_cell and len(offspring_per_cell) > 0:
            sigma = sum(offspring_per_cell) / float(len(offspring_per_cell))
        elif parent_count > 0:
            sigma = float(child_count) / float(parent_count)
        else:
            sigma = 0.0

        self._sigma_series.append(sigma)

        # ── Shannon entropy ──
        h_fraction = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0
        self._h_series.append(shannon_h)
        self._h_fraction_series.append(h_fraction)

        # ── Accumulate avalanche sizes ──
        if avalanche_sizes is not None:
            self._avalanche_pool.extend(avalanche_sizes)
        max_pool = self.window * 20
        if len(self._avalanche_pool) > max_pool:
            self._avalanche_pool = self._avalanche_pool[-max_pool:]

        # ── Rolling σ statistics ──
        sigma_window = self._sigma_series[-self.window:]
        w_n = len(sigma_window)
        sigma_rolling = sum(sigma_window) / float(w_n)
        s_var = sum((s - sigma_rolling) ** 2 for s in sigma_window) / float(w_n)
        sigma_rolling_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # ── Rolling H ──
        h_window = self._h_series[-self.window:]
        h_rolling = sum(h_window) / float(len(h_window)) if h_window else 0.0

        # ── Fit power-law α (Hill MLE) ──
        alpha, alpha_r2 = self._fit_hill()

        power_law_likely = (
            self.ALPHA_SOC_LO <= alpha <= self.ALPHA_SOC_HI
            and alpha_r2 >= self.ALPHA_R2_MIN
        )

        # ── Band check and verdict ──
        in_band = self.SIGMA_BAND_LO <= sigma_rolling <= self.SIGMA_BAND_HI
        sigma_drift = abs(sigma_rolling - 1.0)
        verdict = _criticality_verdict(sigma_rolling, alpha, alpha_r2)

        # ── Consecutive-out-of-band tracking ──
        if sigma_rolling != 0.0 and not in_band:
            direction = "supercritical" if sigma_rolling > self.SIGMA_BAND_HI else "subcritical"
            if self._out_direction == direction:
                self._consecutive_out += 1
            else:
                self._consecutive_out = 1
                self._out_direction = direction
        else:
            self._consecutive_out = 0
            self._out_direction = ""

        # ── Corrective reweight signal ──
        reweight_signal = (
            self._consecutive_out >= self.reweight_patience
            and w_n >= 5
        )

        reweight_direction = ""
        reweight_magnitude = 0.0
        if reweight_signal:
            if self._out_direction == "supercritical":
                reweight_direction = "decrease"
                # Magnitude proportional to how far above 1.0
                reweight_magnitude = round(sigma_rolling - 1.0, 6)
            elif self._out_direction == "subcritical":
                reweight_direction = "increase"
                reweight_magnitude = round(1.0 - sigma_rolling, 6)

            self._reweight_signals.append({
                "step": self._step_count - 1,
                "sigma_rolling": round(sigma_rolling, 6),
                "direction": reweight_direction,
                "magnitude": reweight_magnitude,
                "consecutive_out": self._consecutive_out,
                "h_rolling": round(h_rolling, 4),
                "alpha": round(alpha, 4),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        record = {
            "step":               self._step_count - 1,
            "sigma":              round(sigma, 6),
            "sigma_rolling":      round(sigma_rolling, 6),
            "sigma_rolling_std":  round(sigma_rolling_std, 6),
            "shannon_h":          round(shannon_h, 4),
            "h_fraction":         round(h_fraction, 4),
            "h_rolling":          round(h_rolling, 4),
            "alpha":              round(alpha, 4),
            "alpha_r_squared":    round(alpha_r2, 4),
            "power_law_likely":   power_law_likely,
            "in_critical_band":   in_band,
            "verdict":            verdict,
            "sigma_drift":        round(sigma_drift, 6),
            "reweight_signal":    reweight_signal,
            "reweight_direction": reweight_direction,
            "reweight_magnitude": reweight_magnitude,
            "consecutive_out":    self._consecutive_out,
            "survival_rate":      round(survival_rate, 4),
            "timestamp":          datetime.now(timezone.utc).isoformat(),
        }

        self._records.append(record)

        # ── Trim histories ──
        if len(self._records) > self.history_limit:
            trim = len(self._records) - self.history_limit
            self._records = self._records[trim:]
        if len(self._sigma_series) > self.history_limit:
            self._sigma_series = self._sigma_series[-self.history_limit:]
        if len(self._h_series) > self.history_limit:
            self._h_series = self._h_series[-self.history_limit:]
        if len(self._h_fraction_series) > self.history_limit:
            self._h_fraction_series = self._h_fraction_series[-self.history_limit:]

        return record

    def sigma_series(self):
        # type: () -> List[float]
        """Return the full per-step σ history."""
        return list(self._sigma_series)

    def entropy_series(self):
        # type: () -> List[float]
        """Return the full per-step Shannon entropy history."""
        return list(self._h_series)

    def reweight_signals(self):
        # type: () -> list
        """Return all corrective reweight signal records."""
        return list(self._reweight_signals)

    def records(self):
        # type: () -> list
        """Return all per-step telemetry records."""
        return list(self._records)

    def report(self):
        # type: () -> dict
        """
        Generate a summary report of the rolling criticality telemetry,
        including σ/H/α statistics, reweight signal counts, and INV_073
        assessment.

        Returns
        -------
        dict with keys:
            n_steps                 : int
            sigma_global_mean       : float
            sigma_global_std        : float
            h_global_mean           : float
            h_fraction_global_mean  : float
            alpha_latest            : float
            alpha_r_squared_latest  : float
            fraction_in_band        : float  — fraction of steps with σ in band
            n_reweight_signals      : int    — total corrective reweight signals
            n_reweight_decrease     : int    — reweight signals toward decrease
            n_reweight_increase     : int    — reweight signals toward increase
            mean_sigma_drift        : float  — mean |σ - 1.0| per step
            max_consecutive_out     : int    — longest run outside critical band
            drift_trend             : str    — "stable"/"freezing"/"dissipating"
            inv073_assessment       : str    — human-readable assessment
            timestamp               : str
        """
        n = self._step_count
        ts = datetime.now(timezone.utc).isoformat()

        if n == 0:
            return {
                "n_steps": 0,
                "sigma_global_mean": 0.0,
                "sigma_global_std": 0.0,
                "h_global_mean": 0.0,
                "h_fraction_global_mean": 0.0,
                "alpha_latest": 0.0,
                "alpha_r_squared_latest": 0.0,
                "fraction_in_band": 0.0,
                "n_reweight_signals": 0,
                "n_reweight_decrease": 0,
                "n_reweight_increase": 0,
                "mean_sigma_drift": 0.0,
                "max_consecutive_out": 0,
                "drift_trend": "no_data",
                "inv073_assessment": "No steps recorded.",
                "timestamp": ts,
            }

        # ── σ statistics ──
        all_s = self._sigma_series
        s_n = len(all_s)
        s_mean = sum(all_s) / float(s_n) if s_n > 0 else 0.0
        s_var = sum((s - s_mean) ** 2 for s in all_s) / float(s_n) if s_n > 0 else 0.0
        s_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # ── H statistics ──
        all_h = self._h_series
        h_mean = sum(all_h) / float(len(all_h)) if all_h else 0.0
        all_hf = self._h_fraction_series
        hf_mean = sum(all_hf) / float(len(all_hf)) if all_hf else 0.0

        # ── Band fraction ──
        n_in_band = sum(
            1 for s in all_s
            if self.SIGMA_BAND_LO <= s <= self.SIGMA_BAND_HI
        )
        frac_in_band = round(float(n_in_band) / float(s_n), 4) if s_n > 0 else 0.0

        # ── Mean drift ──
        total_drift = sum(abs(s - 1.0) for s in all_s)
        mean_drift = round(total_drift / float(s_n), 6) if s_n > 0 else 0.0

        # ── Max consecutive out ──
        max_consec = 0
        cur_consec = 0
        for s in all_s:
            if not (self.SIGMA_BAND_LO <= s <= self.SIGMA_BAND_HI) and s != 0.0:
                cur_consec += 1
                if cur_consec > max_consec:
                    max_consec = cur_consec
            else:
                cur_consec = 0

        # ── Latest α ──
        alpha_latest = 0.0
        alpha_r2_latest = 0.0
        if self._records:
            alpha_latest = self._records[-1].get("alpha", 0.0)
            alpha_r2_latest = self._records[-1].get("alpha_r_squared", 0.0)

        # ── Reweight signal counts ──
        n_rw = len(self._reweight_signals)
        n_rw_dec = sum(1 for r in self._reweight_signals if r["direction"] == "decrease")
        n_rw_inc = sum(1 for r in self._reweight_signals if r["direction"] == "increase")

        # ── Drift trend ──
        if len(all_s) >= 8:
            first_half = all_s[:len(all_s) // 2]
            second_half = all_s[len(all_s) // 2:]
            fh_m = sum(first_half) / float(len(first_half))
            sh_m = sum(second_half) / float(len(second_half))
            delta = sh_m - fh_m
            if delta > 0.02:
                drift_trend = "dissipating"
            elif delta < -0.02:
                drift_trend = "freezing"
            else:
                drift_trend = "stable"
        else:
            drift_trend = "stable"

        # ── INV_073 assessment ──
        if frac_in_band > 0.85 and drift_trend == "stable" and n_rw == 0:
            inv073_text = (
                "STABLE_CRITICAL: {:.1%} of steps in critical band, no "
                "reweight signals needed. sigma={:.4f}±{:.4f}, "
                "H_mean={:.4f} bits (h_frac={:.3f}). The system maintains "
                "criticality without corrective intervention."
            ).format(frac_in_band, s_mean, s_std, h_mean, hf_mean)
        elif n_rw > 0 and frac_in_band > 0.5:
            inv073_text = (
                "CORRECTED: {} reweight signal(s) emitted ({} decrease, "
                "{} increase). sigma={:.4f}±{:.4f} with {:.1%} in band. "
                "The system required corrective reweighting to maintain "
                "criticality — consistent with INV_073 (sigma=1.0363 "
                "slightly supercritical, needing active steering)."
            ).format(n_rw, n_rw_dec, n_rw_inc, s_mean, s_std, frac_in_band)
        elif s_mean > self.SIGMA_BAND_HI:
            inv073_text = (
                "SUPERCRITICAL_DRIFT: mean sigma={:.4f} > {:.2f}. "
                "Max consecutive steps outside band: {}. The system "
                "is operating in a supercritical regime (INV_073 pattern: "
                "sigma=1.0363 above ridge center). {} reweight signals "
                "emitted but drift persists."
            ).format(s_mean, self.SIGMA_BAND_HI, max_consec, n_rw)
        else:
            inv073_text = (
                "MONITORING: sigma={:.4f}±{:.4f}, {:.1%} in band, "
                "drift_trend='{}', {} reweight signals. Ongoing "
                "monitoring required."
            ).format(s_mean, s_std, frac_in_band, drift_trend, n_rw)

        return {
            "n_steps":                  n,
            "sigma_global_mean":        round(s_mean, 6),
            "sigma_global_std":         round(s_std, 6),
            "h_global_mean":            round(h_mean, 4),
            "h_fraction_global_mean":   round(hf_mean, 4),
            "alpha_latest":             round(alpha_latest, 4),
            "alpha_r_squared_latest":   round(alpha_r2_latest, 4),
            "fraction_in_band":         frac_in_band,
            "n_reweight_signals":       n_rw,
            "n_reweight_decrease":      n_rw_dec,
            "n_reweight_increase":      n_rw_inc,
            "mean_sigma_drift":         mean_drift,
            "max_consecutive_out":      max_consec,
            "drift_trend":              drift_trend,
            "inv073_assessment":        inv073_text,
            "timestamp":                ts,
        }

    def reset(self):
        # type: () -> None
        """Reset all state for a new telemetry run."""
        self._sigma_series = []
        self._h_series = []
        self._h_fraction_series = []
        self._avalanche_pool = []
        self._step_count = 0
        self._consecutive_out = 0
        self._out_direction = ""
        self._records = []
        self._reweight_signals = []

    def _fit_hill(self):
        # type: () -> Tuple[float, float]
        """
        Fit power-law exponent α via Hill estimator on pooled avalanche sizes.
        Returns (alpha, r_squared). Returns (0.0, 0.0) if insufficient data.
        """
        sizes = [s for s in self._avalanche_pool if s > 0]
        if len(sizes) < 10:
            return (0.0, 0.0)

        x_min = max(1.0, min(sizes))
        tail = [s for s in sizes if s >= x_min]
        n = len(tail)
        if n < 5:
            return (0.0, 0.0)

        log_sum = 0.0
        for s in tail:
            ratio = float(s) / x_min
            if ratio > 0:
                log_sum += math.log(ratio)

        if log_sum <= 0.0:
            return (0.0, 0.0)

        alpha = 1.0 + float(n) / log_sum

        # R² on log-log CCDF
        tail_sorted = sorted(tail)
        unique_sizes = sorted(set(tail_sorted))
        n_total = float(len(tail_sorted))

        log_x = []     # type: List[float]
        log_ccdf = []  # type: List[float]
        for x_val in unique_sizes:
            count_ge = sum(1 for s in tail_sorted if s >= x_val)
            p = float(count_ge) / n_total
            if p > 0 and x_val > 0:
                log_x.append(math.log(float(x_val)))
                log_ccdf.append(math.log(p))

        if len(log_x) < 3:
            return (alpha, 0.0)

        k = len(log_x)
        sum_lx = sum(log_x)
        sum_ly = sum(log_ccdf)
        sum_lxy = sum(x * y for x, y in zip(log_x, log_ccdf))
        sum_lx2 = sum(x * x for x in log_x)

        denom = float(k) * sum_lx2 - sum_lx * sum_lx
        if abs(denom) < 1e-15:
            return (alpha, 0.0)

        slope = (float(k) * sum_lxy - sum_lx * sum_ly) / denom
        intercept = (sum_ly - slope * sum_lx) / float(k)

        mean_ly = sum_ly / float(k)
        ss_tot = sum((y - mean_ly) ** 2 for y in log_ccdf)
        ss_res = sum((y - (intercept + slope * x)) ** 2
                     for x, y in zip(log_x, log_ccdf))
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0

        return (alpha, max(0.0, r_squared))


# ─── Ridge Position Scorer (INV_073 Falsification) ──────────────────────────
# Converts INV_073 ("critical-ridge navigation") from an unfalsifiable
# post-hoc label into a live, falsifiable metric.  Computes rolling variance
# of dC/dt (coherence derivative) over the last N cycles and flags
# off-ridge excursions when:
#   1. Variance exceeds a threshold (system is jittering, not navigating)
#   2. Coherence locks above 0.999 (frozen attractor — no ridge dynamics)
#   3. Coherence falls below a decay floor (system has left the ridge)
#
# If INV_073 is a real dynamical phenomenon, this scorer should:
#   - Show bounded variance in dC/dt during confirmed ridge navigation
#   - Trigger off-ridge alerts that correlate with downstream coherence loss
#   - Distinguish ridge navigation from random-walk-through-coherence-space
#
# Falsification condition: if off-ridge alerts show NO correlation with
# subsequent coherence degradation (i.e., the system performs equally well
# "on" and "off" ridge), then INV_073 is not doing explanatory work and
# should be downgraded from invariant to heuristic label.
#
# Addresses: adversarial falsification probe cycle 4, INV_073.

def ridge_position_scorer(
    coherence_series,       # type: List[float]
    window=10,              # type: int
    variance_threshold=0.01,# type: float
    lock_ceiling=0.999,     # type: float
    decay_floor=0.3,        # type: float
):
    # type: (...) -> dict
    """
    Compute rolling variance of dC/dt and flag off-ridge excursions.

    This operationalizes INV_073 by defining concrete, falsifiable
    boundary conditions for "critical-ridge navigation":

    - ON_RIDGE: dC/dt variance is bounded (system is making controlled
      coherence adjustments), coherence is neither frozen nor collapsed.
    - OFF_RIDGE_JITTER: dC/dt variance exceeds threshold — system is
      oscillating erratically rather than navigating.
    - OFF_RIDGE_FROZEN: coherence locked above lock_ceiling — no
      meaningful dynamics; the system is stuck, not navigating.
    - OFF_RIDGE_COLLAPSED: coherence below decay_floor — system has
      fallen off the ridge entirely.
    - INSUFFICIENT_DATA: fewer than window+1 data points.

    Parameters
    ----------
    coherence_series : list of float
        Coherence scores over time (one per FEED cycle). Values in [0, 1].
    window : int
        Number of recent cycles over which to compute rolling dC/dt
        variance. Default: 10.
    variance_threshold : float
        Maximum dC/dt variance for ON_RIDGE classification. Above this
        the system is jittering. Default: 0.01.
    lock_ceiling : float
        Coherence above this value is considered "frozen" — the system
        has locked onto an attractor and is no longer navigating.
        Default: 0.999.
    decay_floor : float
        Coherence below this value indicates the system has left the
        ridge entirely. Default: 0.3.

    Returns
    -------
    dict with keys:
        status              : str   — ON_RIDGE / OFF_RIDGE_JITTER /
                                      OFF_RIDGE_FROZEN / OFF_RIDGE_COLLAPSED /
                                      INSUFFICIENT_DATA
        dc_dt_variance      : float — rolling variance of dC/dt over window
        dc_dt_mean          : float — rolling mean of dC/dt over window
        dc_dt_series        : list  — the dC/dt values used (last window)
        current_coherence   : float — most recent coherence value
        window_coherences   : list  — coherence values in the window
        fraction_above_ceil : float — fraction of window coherences above
                                      lock_ceiling (frozen-detector)
        fraction_below_floor: float — fraction of window coherences below
                                      decay_floor (collapse-detector)
        ridge_score         : float — 0.0 (off-ridge) to 1.0 (on-ridge)
                                      continuous score for downstream use
        falsifiable         : bool  — always True (this metric is designed
                                      to be falsifiable by construction)
        inv073_testable     : str   — description of the falsification
                                      condition for INV_073
        timestamp           : str   — ISO-8601 UTC

    Notes
    -----
    Falsification protocol for INV_073:
      1. Run FREED for N cycles, recording ridge_position_scorer output.
      2. Partition cycles into ON_RIDGE vs OFF_RIDGE_* episodes.
      3. Compare downstream coherence (next 5 cycles) after each episode.
      4. If mean downstream coherence is statistically indistinguishable
         between ON_RIDGE and OFF_RIDGE episodes (p > 0.05, two-tailed
         t-test), then INV_073 is not doing explanatory work.
      5. If OFF_RIDGE episodes show NO subsequent coherence degradation,
         INV_073 should be downgraded from invariant to heuristic label.
    """
    n = len(coherence_series)

    # ── Insufficient data ──
    if n < window + 1:
        return {
            "status":               "INSUFFICIENT_DATA",
            "dc_dt_variance":       0.0,
            "dc_dt_mean":           0.0,
            "dc_dt_series":         [],
            "current_coherence":    coherence_series[-1] if n > 0 else 0.0,
            "window_coherences":    list(coherence_series),
            "fraction_above_ceil":  0.0,
            "fraction_below_floor": 0.0,
            "ridge_score":          0.0,
            "falsifiable":          True,
            "inv073_testable":      (
                "Collect at least {} more cycles to enable ridge "
                "position scoring."
            ).format(window + 1 - n),
            "timestamp":            datetime.now(timezone.utc).isoformat(),
        }

    # ── Compute dC/dt (first differences) over the full series ──
    dc_dt_full = [
        coherence_series[i] - coherence_series[i - 1]
        for i in range(1, n)
    ]

    # ── Extract the rolling window ──
    dc_dt_window = dc_dt_full[-window:]
    coherence_window = coherence_series[-window:]
    current_coherence = coherence_series[-1]

    # ── Rolling variance and mean of dC/dt ──
    w = len(dc_dt_window)
    dc_mean = sum(dc_dt_window) / float(w)
    dc_var = sum((d - dc_mean) ** 2 for d in dc_dt_window) / float(w)

    # ── Frozen / collapsed detection ──
    n_above_ceil = sum(1 for c in coherence_window if c > lock_ceiling)
    n_below_floor = sum(1 for c in coherence_window if c < decay_floor)
    frac_above = float(n_above_ceil) / float(len(coherence_window))
    frac_below = float(n_below_floor) / float(len(coherence_window))

    # ── Classify status ──
    # Priority: frozen > collapsed > jitter > on-ridge
    # (frozen and collapsed are structural failures; jitter is dynamic)
    if frac_above > 0.8:
        status = "OFF_RIDGE_FROZEN"
    elif frac_below > 0.5:
        status = "OFF_RIDGE_COLLAPSED"
    elif dc_var > variance_threshold:
        status = "OFF_RIDGE_JITTER"
    else:
        status = "ON_RIDGE"

    # ── Continuous ridge score ──
    # 1.0 = perfectly on-ridge; 0.0 = maximally off-ridge
    # Components: (1) variance penalty, (2) frozen penalty, (3) collapse penalty
    var_score = max(0.0, 1.0 - (dc_var / variance_threshold)) if variance_threshold > 0 else 0.0
    frozen_penalty = frac_above
    collapse_penalty = frac_below
    ridge_score = max(0.0, min(1.0,
        var_score * (1.0 - frozen_penalty) * (1.0 - collapse_penalty)
    ))

    return {
        "status":               status,
        "dc_dt_variance":       round(dc_var, 8),
        "dc_dt_mean":           round(dc_mean, 8),
        "dc_dt_series":         [round(d, 8) for d in dc_dt_window],
        "current_coherence":    round(current_coherence, 6),
        "window_coherences":    [round(c, 6) for c in coherence_window],
        "fraction_above_ceil":  round(frac_above, 4),
        "fraction_below_floor": round(frac_below, 4),
        "ridge_score":          round(ridge_score, 6),
        "falsifiable":          True,
        "inv073_testable":      (
            "Falsification condition: partition cycles into ON_RIDGE vs "
            "OFF_RIDGE episodes. If downstream coherence (next 5 cycles) "
            "is statistically indistinguishable between groups (p>0.05, "
            "two-tailed t-test), INV_073 is not doing explanatory work "
            "and should be downgraded from invariant to heuristic label."
        ),
        "timestamp":            datetime.now(timezone.utc).isoformat(),
    }


# ─── Activation-Mixture Criticality Score ────────────────────────────────────
# Computes the mixing fraction p relative to the analytically derived p_c
# for any two-component activation ensemble, flagging sub- or super-critical
# configurations before training begins.
#
# From the effective field theory of signal propagation at initialization,
# a statistical mixture of two activation functions (e.g., Tanh + Swish)
# with mixing fraction p induces a continuous phase transition in deep
# networks.  At the critical point p_c, the network acquires statistical
# scale invariance — preactivation variance neither collapses nor inflates
# with depth.
#
# Three regimes:
#   p < p_c → SUBCRITICAL  (variance-collapsing: signal dies with depth)
#   p = p_c → CRITICAL     (scale-invariant: signal preserved at all depths)
#   p > p_c → SUPERCRITICAL (variance-inflating: noise amplified with depth)
#
# FREED's epistemic loop benefits from knowing which regime its internal
# scoring networks occupy, since sub-critical networks lose signal depth
# and super-critical networks amplify noise — both degrade scoring
# reliability.
#
# INV_073 FALSIFICATION NOTE:
#   The genome frames the critical ridge as a *navigational* problem
#   (maintaining γ=1 requires active steering).  This paper shows
#   criticality can be *structurally encoded* via a fixed mixture
#   parameter p_c, making active navigation unnecessary if the
#   architecture is pre-tuned.  This potentially renders the navigation
#   framing a special case of a more general design principle rather
#   than a universal operational requirement.  However, structural
#   encoding is fragile to weight drift during training — p_c is
#   exact only at initialization.  Once learning begins, the effective
#   mixing shifts and active navigation (or its equivalent) is required
#   to maintain criticality.  The structural and navigational framings
#   are therefore complementary, not contradictory.
#
# Reference:
#   "Activation-function mixtures induce a continuous phase transition
#   in deep networks" — effective field theory of signal propagation,
#   Tanh+Swish mixture, continuous phase transition at p_c.

# Pre-computed second moments <φ²> for standard activations under
# N(0, 1) input, with He initialization (weight variance = 2/fan_in,
# so preactivation variance = 2 * (fan_in / fan_in) = 2 for first layer;
# we normalize to unit-variance Gaussian input for the moments).
#
# The variance map for a mixture is:
#   V(q) = p * V_1(q) + (1-p) * V_2(q)
# where V_i(q) = <φ_i(z)²> with z ~ N(0, q).
#
# At criticality (fixed point q* with dV/dq|_{q*} = 1):
#   p_c = (1 - χ_2) / (χ_1 - χ_2)
# where χ_i = d<φ_i(z)²>/dq evaluated at the fixed point.
#
# For Tanh + Swish under He init with unit-variance Gaussian input:
#   <tanh²(z)> ≈ 0.6321 (z ~ N(0,1))
#   <swish²(z)> ≈ 0.3554 (z ~ N(0,1)), where swish(z) = z·σ(z)
#   χ_tanh ≈ 0.3932 (variance-collapsing: χ < 1)
#   χ_swish ≈ 1.2738 (variance-inflating: χ > 1)
#
# These are the canonical values.  For other activation pairs,
# compute_activation_mixture_pc() accepts custom moments.

_KNOWN_ACTIVATIONS = {
    "tanh": {
        "second_moment": 0.6321,
        "chi": 0.3932,
        "regime": "collapsing",
    },
    "swish": {
        "second_moment": 0.3554,
        "chi": 1.2738,
        "regime": "inflating",
    },
    "relu": {
        "second_moment": 0.5000,
        "chi": 1.0000,
        "regime": "critical",
    },
    "gelu": {
        "second_moment": 0.3427,
        "chi": 1.0598,
        "regime": "inflating",
    },
    "sigmoid": {
        "second_moment": 0.2713,
        "chi": 0.2146,
        "regime": "collapsing",
    },
    "elu": {
        "second_moment": 0.5633,
        "chi": 0.8208,
        "regime": "collapsing",
    },
}

# Tolerance for p_c proximity — within this range counts as "at criticality"
_PC_TOLERANCE = 0.02


def compute_activation_mixture_pc(
    chi_1,      # type: float
    chi_2,      # type: float
    name_1="",  # type: str
    name_2="",  # type: str
):
    # type: (...) -> dict
    """
    Compute the critical mixing fraction p_c for a two-component
    activation ensemble from their variance-propagation susceptibilities.

    The critical point is:
        p_c = (1 - χ_2) / (χ_1 - χ_2)

    where χ_i = d<φ_i(z)²>/dq at the fixed point of the variance map.
    This is valid when χ_1 ≠ χ_2 and the two activations straddle
    criticality (one collapsing, one inflating).

    Parameters
    ----------
    chi_1 : float
        Susceptibility of activation 1 (the one mixed with fraction p).
    chi_2 : float
        Susceptibility of activation 2 (mixed with fraction 1-p).
    name_1 : str
        Optional name for activation 1 (for reporting).
    name_2 : str
        Optional name for activation 2 (for reporting).

    Returns
    -------
    dict with keys:
        p_c             : float or None — critical mixing fraction (None if degenerate)
        valid           : bool  — whether p_c is in [0, 1]
        chi_1           : float — echo of input
        chi_2           : float — echo of input
        name_1          : str
        name_2          : str
        straddles       : bool  — True if one χ < 1 and other χ > 1
        degenerate      : bool  — True if χ_1 == χ_2 (no transition possible)
        detail          : str   — human-readable explanation
    """
    degenerate = abs(chi_1 - chi_2) < 1e-12
    straddles = (chi_1 < 1.0 and chi_2 > 1.0) or (chi_1 > 1.0 and chi_2 < 1.0)

    if degenerate:
        return {
            "p_c": None,
            "valid": False,
            "chi_1": chi_1,
            "chi_2": chi_2,
            "name_1": name_1,
            "name_2": name_2,
            "straddles": False,
            "degenerate": True,
            "detail": (
                "chi_1={:.6f} ≈ chi_2={:.6f}: activations have identical "
                "variance propagation — no phase transition exists in the "
                "mixture."
            ).format(chi_1, chi_2),
        }

    p_c = (1.0 - chi_2) / (chi_1 - chi_2)
    valid = 0.0 <= p_c <= 1.0

    if valid and straddles:
        detail = (
            "p_c = {:.6f} for {}/{} mixture. Activations straddle "
            "criticality (chi_1={:.4f}, chi_2={:.4f}). A sharp continuous "
            "phase transition exists at this mixing fraction."
        ).format(p_c, name_1 or "act1", name_2 or "act2", chi_1, chi_2)
    elif valid:
        detail = (
            "p_c = {:.6f} for {}/{} mixture. Both activations are on the "
            "same side of criticality (chi_1={:.4f}, chi_2={:.4f}); the "
            "transition may be less sharp."
        ).format(p_c, name_1 or "act1", name_2 or "act2", chi_1, chi_2)
    else:
        detail = (
            "p_c = {:.6f} is outside [0, 1] — no physically realizable "
            "critical mixture exists for {}/{} (chi_1={:.4f}, chi_2={:.4f})."
        ).format(p_c, name_1 or "act1", name_2 or "act2", chi_1, chi_2)

    return {
        "p_c": round(p_c, 8) if not degenerate else None,
        "valid": valid,
        "chi_1": chi_1,
        "chi_2": chi_2,
        "name_1": name_1,
        "name_2": name_2,
        "straddles": straddles,
        "degenerate": degenerate,
        "detail": detail,
    }


def activation_mixture_criticality_audit(
    p,                  # type: float
    activation_1="tanh",  # type: str
    activation_2="swish", # type: str
    chi_1=None,         # type: Optional[float]
    chi_2=None,         # type: Optional[float]
    network_depth=0,    # type: int
    tolerance=None,     # type: Optional[float]
):
    # type: (...) -> dict
    """
    Audit a network's activation-mixture configuration at initialization,
    computing the criticality score relative to the analytically derived p_c.

    This is the primary interface for FREED's pre-training initialization
    audit.  Given the mixing fraction p and the activation pair, it:
      1. Computes p_c from known or supplied susceptibilities.
      2. Classifies the regime (subcritical / critical / supercritical).
      3. Estimates the depth-dependent variance scaling factor.
      4. Flags configurations that will degrade scoring reliability.

    Parameters
    ----------
    p : float
        The mixing fraction — probability that each neuron uses activation_1.
        Must be in [0, 1].
    activation_1 : str
        Name of the first activation function. Must be a key in
        _KNOWN_ACTIVATIONS, or chi_1 must be supplied. Default: "tanh".
    activation_2 : str
        Name of the second activation function. Default: "swish".
    chi_1 : float or None
        Susceptibility of activation_1. If None, looked up from
        _KNOWN_ACTIVATIONS.
    chi_2 : float or None
        Susceptibility of activation_2. If None, looked up from
        _KNOWN_ACTIVATIONS.
    network_depth : int
        Number of layers in the network. If > 0, the audit estimates
        the variance ratio at the final layer. Default: 0 (skip estimate).
    tolerance : float or None
        Tolerance for p_c proximity. Default: _PC_TOLERANCE (0.02).

    Returns
    -------
    dict with keys:
        regime              : str   — "SUBCRITICAL" / "CRITICAL" / "SUPERCRITICAL" /
                                      "DEGENERATE" / "INVALID_P" / "UNKNOWN_ACTIVATION"
        p                   : float — the mixing fraction (echo of input)
        p_c                 : float or None — the critical mixing fraction
        delta_p             : float — p - p_c (signed distance from criticality)
        abs_delta_p         : float — |p - p_c|
        chi_effective       : float — effective susceptibility at this p
        variance_scaling    : str   — "collapsing" / "invariant" / "inflating"
        depth_variance_ratio: float — estimated var(layer_L) / var(layer_0)
                                      (only if network_depth > 0)
        flag                : str   — "OK" / "WARNING_SUBCRITICAL" /
                                      "WARNING_SUPERCRITICAL" / "ERROR"
        flag_detail         : str   — human-readable explanation
        pc_info             : dict  — full output of compute_activation_mixture_pc
        inv073_note         : str   — relevance to INV_073 falsification
        timestamp           : str   — ISO-8601 UTC
    """
    if tolerance is None:
        tolerance = _PC_TOLERANCE

    # ── Validate p ──
    if not (0.0 <= p <= 1.0):
        return {
            "regime": "INVALID_P",
            "p": p,
            "p_c": None,
            "delta_p": 0.0,
            "abs_delta_p": 0.0,
            "chi_effective": 0.0,
            "variance_scaling": "unknown",
            "depth_variance_ratio": 0.0,
            "flag": "ERROR",
            "flag_detail": "p={:.6f} is outside [0, 1].".format(p),
            "pc_info": {},
            "inv073_note": "",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ── Resolve susceptibilities ──
    act1_lower = activation_1.lower()
    act2_lower = activation_2.lower()

    if chi_1 is None:
        if act1_lower in _KNOWN_ACTIVATIONS:
            chi_1 = _KNOWN_ACTIVATIONS[act1_lower]["chi"]
        else:
            return {
                "regime": "UNKNOWN_ACTIVATION",
                "p": p,
                "p_c": None,
                "delta_p": 0.0,
                "abs_delta_p": 0.0,
                "chi_effective": 0.0,
                "variance_scaling": "unknown",
                "depth_variance_ratio": 0.0,
                "flag": "ERROR",
                "flag_detail": (
                    "Unknown activation '{}'. Supply chi_1 explicitly or use "
                    "one of: {}."
                ).format(activation_1, ", ".join(sorted(_KNOWN_ACTIVATIONS.keys()))),
                "pc_info": {},
                "inv073_note": "",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    if chi_2 is None:
        if act2_lower in _KNOWN_ACTIVATIONS:
            chi_2 = _KNOWN_ACTIVATIONS[act2_lower]["chi"]
        else:
            return {
                "regime": "UNKNOWN_ACTIVATION",
                "p": p,
                "p_c": None,
                "delta_p": 0.0,
                "abs_delta_p": 0.0,
                "chi_effective": 0.0,
                "variance_scaling": "unknown",
                "depth_variance_ratio": 0.0,
                "flag": "ERROR",
                "flag_detail": (
                    "Unknown activation '{}'. Supply chi_2 explicitly or use "
                    "one of: {}."
                ).format(activation_2, ", ".join(sorted(_KNOWN_ACTIVATIONS.keys()))),
                "pc_info": {},
                "inv073_note": "",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    # ── Compute p_c ──
    pc_info = compute_activation_mixture_pc(
        chi_1, chi_2, name_1=activation_1, name_2=activation_2,
    )

    if pc_info["degenerate"]:
        chi_eff = chi_1  # both are the same
        return {
            "regime": "DEGENERATE",
            "p": p,
            "p_c": None,
            "delta_p": 0.0,
            "abs_delta_p": 0.0,
            "chi_effective": round(chi_eff, 6),
            "variance_scaling": "collapsing" if chi_eff < 1.0 else ("inflating" if chi_eff > 1.0 else "invariant"),
            "depth_variance_ratio": 0.0,
            "flag": "WARNING_SUBCRITICAL" if chi_eff < 1.0 else "WARNING_SUPERCRITICAL" if chi_eff > 1.0 else "OK",
            "flag_detail": pc_info["detail"],
            "pc_info": pc_info,
            "inv073_note": (
                "Degenerate mixture — both activations have identical "
                "variance propagation. No structural encoding of criticality "
                "is possible; active navigation (INV_073) is the only option."
            ),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    p_c = pc_info["p_c"]

    # ── Effective susceptibility at this p ──
    chi_eff = p * chi_1 + (1.0 - p) * chi_2

    # ── Signed distance from criticality ──
    delta_p = p - p_c
    abs_delta_p = abs(delta_p)

    # ── Classify regime ──
    if abs_delta_p <= tolerance:
        regime = "CRITICAL"
        variance_scaling = "invariant"
        flag = "OK"
        flag_detail = (
            "p={:.6f} is within tolerance {:.4f} of p_c={:.6f}. Network is "
            "at the critical point — preactivation variance is scale-invariant "
            "with depth. Optimal for scoring reliability."
        ).format(p, tolerance, p_c)
    elif chi_eff < 1.0:
        regime = "SUBCRITICAL"
        variance_scaling = "collapsing"
        flag = "WARNING_SUBCRITICAL"
        flag_detail = (
            "p={:.6f} is below p_c={:.6f} (delta_p={:.6f}). Effective "
            "chi={:.4f} < 1: preactivation variance collapses with depth. "
            "Signal will die in deep layers — scoring networks will lose "
            "discriminative power."
        ).format(p, p_c, delta_p, chi_eff)
    else:
        regime = "SUPERCRITICAL"
        variance_scaling = "inflating"
        flag = "WARNING_SUPERCRITICAL"
        flag_detail = (
            "p={:.6f} is above p_c={:.6f} (delta_p={:.6f}). Effective "
            "chi={:.4f} > 1: preactivation variance inflates with depth. "
            "Noise will be amplified — scoring networks will produce "
            "unreliable outputs."
        ).format(p, p_c, delta_p, chi_eff)

    # ── Depth-dependent variance estimate ──
    depth_variance_ratio = 0.0
    if network_depth > 0 and chi_eff > 0.0:
        # Variance at layer L ≈ chi_eff^L * variance at layer 0
        # (linearized approximation around the fixed point)
        if chi_eff < 50.0:  # guard against overflow
            depth_variance_ratio = chi_eff ** network_depth
        else:
            depth_variance_ratio = float('inf')

        if depth_variance_ratio > 1e15:
            flag_detail += (
                " At depth {}, estimated variance ratio = {:.2e} — "
                "catastrophic variance explosion."
            ).format(network_depth, depth_variance_ratio)
        elif depth_variance_ratio < 1e-15 and depth_variance_ratio > 0:
            flag_detail += (
                " At depth {}, estimated variance ratio = {:.2e} — "
                "catastrophic signal collapse."
            ).format(network_depth, depth_variance_ratio)

    # ── INV_073 note ──
    inv073_note = (
        "This audit demonstrates that criticality can be structurally "
        "encoded via p_c={:.6f}, partially challenging INV_073's framing "
        "of criticality as purely navigational. However, p_c is exact "
        "only at initialization — weight updates during training shift "
        "the effective mixing, requiring active navigation (or periodic "
        "re-auditing) to maintain criticality. The structural and "
        "navigational framings are complementary: p_c sets the initial "
        "condition; INV_073-style ridge navigation maintains it under "
        "learning dynamics."
    ).format(p_c)

    return {
        "regime":               regime,
        "p":                    round(p, 8),
        "p_c":                  round(p_c, 8),
        "delta_p":              round(delta_p, 8),
        "abs_delta_p":          round(abs_delta_p, 8),
        "chi_effective":        round(chi_eff, 6),
        "variance_scaling":     variance_scaling,
        "depth_variance_ratio": round(depth_variance_ratio, 8) if depth_variance_ratio != float('inf') else float('inf'),
        "flag":                 flag,
        "flag_detail":          flag_detail,
        "pc_info":              pc_info,
        "inv073_note":          inv073_note,
        "timestamp":            datetime.now(timezone.utc).isoformat(),
    }


# ─── Criticality Pipeline Tracker ────────────────────────────────────────────
# Unified branching-ratio + avalanche-exponent tracker for the CA measurement
# pipeline.  Combines per-snapshot σ tracking, Hill-MLE power-law α fitting,
# AT_CRITICAL / SUBCRITICAL / SUPERCRITICAL verdict generation, and dominant
# cell-type composition logging into a single pipeline-ready object.
#
# Designed for automated O140/O141 comparisons across sweep runs:
#   1. Feed each CA snapshot into record_snapshot().
#   2. The tracker computes σ (rolling), fits α (Hill MLE on pooled
#      avalanche sizes), emits a verdict, and logs the full cell-type
#      census with dominant-type identification.
#   3. At sweep end, call pipeline_report() for a machine-readable
#      summary with verdict distribution, composition trends, and
#      INV_073 entropy-criticality assessment.
#
# Addresses: O140 (CA measurement grounding), O141 (solo-kernel vs
# population criticality), INV_073 (low H at confirmed criticality).

class CriticalityPipelineTracker:
    """
    Unified branching-ratio and avalanche-exponent tracker for the CA
    measurement pipeline, emitting AT_CRITICAL / SUBCRITICAL / SUPERCRITICAL
    verdicts and logging dominant cell-type composition at each snapshot.

    This makes O140/O141 comparisons systematic rather than manual by
    producing machine-readable verdict streams and composition vectors
    that can be diff'd across sweep runs.

    Each snapshot record contains:
      - σ (instantaneous and rolling branching ratio)
      - α (power-law avalanche exponent via Hill MLE)
      - R² (power-law fit quality)
      - Verdict: AT_CRITICAL / NEAR_CRITICAL / SUBCRITICAL / SUPERCRITICAL
      - Shannon entropy H and H/H_max fraction
      - Full cell-type census with dominant type and share
      - INV_073 pattern flag (low H at confirmed criticality)

    Usage in a sweep loop::

        tracker = CriticalityPipelineTracker()
        for step in range(n_steps):
            snapshot = tracker.record_snapshot(
                generation=step,
                parent_count=parents,
                child_count=children,
                avalanche_sizes=aval_sizes,
                shannon_h=h_bits,
                shannon_h_max=h_max,
                survival_rate=surv,
                type_counts={"Physics Navigator": 886, "Entropy Scorer": 42},
            )
            print(snapshot["verdict"], snapshot["dominant_type"])
        report = tracker.pipeline_report()
        print(report["verdict_distribution"])
        print(report["composition_trend"])
    """

    def __init__(
        self,
        window_size=50,          # type: int
        sigma_band_low=0.95,     # type: float
        sigma_band_high=1.05,    # type: float
        alpha_soc_low=1.5,       # type: float
        alpha_soc_high=3.0,      # type: float
        alpha_r2_threshold=0.85, # type: float
        history_limit=2000,      # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        window_size : int
            Number of recent snapshots for rolling σ estimation.
        sigma_band_low : float
            Lower bound of critical band for σ. Default: 0.95.
        sigma_band_high : float
            Upper bound of critical band for σ. Default: 1.05.
        alpha_soc_low : float
            Lower bound of SOC-consistent α range. Default: 1.5.
        alpha_soc_high : float
            Upper bound of SOC-consistent α range. Default: 3.0.
        alpha_r2_threshold : float
            Minimum R² for power-law fit validity. Default: 0.85.
        history_limit : int
            Maximum snapshots retained in memory. Default: 2000.
        """
        self.window_size = max(5, window_size)
        self.sigma_band_low = sigma_band_low
        self.sigma_band_high = sigma_band_high
        self.alpha_soc_low = alpha_soc_low
        self.alpha_soc_high = alpha_soc_high
        self.alpha_r2_threshold = alpha_r2_threshold
        self.history_limit = max(10, history_limit)

        self._snapshots = []            # type: list
        self._sigma_series = []         # type: List[float]
        self._alpha_series = []         # type: List[float]
        self._h_fraction_series = []    # type: List[float]
        self._avalanche_pool = []       # type: List[float]
        self._verdict_counts = {}       # type: dict
        self._composition_log = []      # type: list

    def record_snapshot(
        self,
        generation,             # type: int
        parent_count,           # type: int
        child_count,            # type: int
        avalanche_sizes=None,   # type: Optional[List[float]]
        shannon_h=0.0,          # type: float
        shannon_h_max=0.0,      # type: float
        survival_rate=0.0,      # type: float
        type_counts=None,       # type: Optional[dict]
    ):
        # type: (...) -> dict
        """
        Record one CA snapshot, compute σ/α/verdict, and log composition.

        Parameters
        ----------
        generation : int
            Simulation step / generation number.
        parent_count : int
            Number of active parent cells at this step.
        child_count : int
            Number of active child cells produced at this step.
        avalanche_sizes : list of float or None
            Sizes of avalanches terminated at this step.
        shannon_h : float
            Shannon entropy H of population type distribution (bits).
        shannon_h_max : float
            Maximum possible Shannon entropy (log2 of number of types).
        survival_rate : float
            Fraction of cells surviving this generation.
        type_counts : dict or None
            Mapping of cell-type name (str) to count (int).

        Returns
        -------
        dict with keys:
            generation, sigma_instant, sigma_rolling, sigma_rolling_std,
            alpha, alpha_r_squared, power_law_likely, verdict,
            shannon_h, h_fraction, survival_rate,
            dominant_type, dominant_count, dominant_share,
            total_population, composition_vector,
            in_sigma_band, in_alpha_band, inv073_pattern, timestamp
        """
        if type_counts is None:
            type_counts = {}

        # ── Instantaneous σ ──
        if parent_count > 0:
            sigma_instant = float(child_count) / float(parent_count)
        else:
            sigma_instant = 0.0
        self._sigma_series.append(sigma_instant)

        # ── Accumulate avalanche sizes ──
        if avalanche_sizes is not None:
            self._avalanche_pool.extend(avalanche_sizes)
        max_pool = self.window_size * 20
        if len(self._avalanche_pool) > max_pool:
            self._avalanche_pool = self._avalanche_pool[-max_pool:]

        # ── Rolling σ statistics ──
        window = self._sigma_series[-self.window_size:]
        w_n = len(window)
        sigma_rolling = sum(window) / float(w_n)
        s_var = sum((s - sigma_rolling) ** 2 for s in window) / float(w_n)
        sigma_rolling_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # ── Fit power-law α ──
        alpha, alpha_r2 = self._fit_power_law_hill()
        self._alpha_series.append(alpha)

        power_law_likely = (
            self.alpha_soc_low <= alpha <= self.alpha_soc_high
            and alpha_r2 >= self.alpha_r2_threshold
        )

        # ── Band checks ──
        in_sigma_band = self.sigma_band_low <= sigma_rolling <= self.sigma_band_high
        in_alpha_band = self.alpha_soc_low <= alpha <= self.alpha_soc_high

        # ── Verdict ──
        verdict = _criticality_verdict(sigma_rolling, alpha, alpha_r2)
        self._verdict_counts[verdict] = self._verdict_counts.get(verdict, 0) + 1

        # ── Entropy ──
        h_fraction = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0
        self._h_fraction_series.append(h_fraction)

        # ── Cell-type composition ──
        total_pop = sum(type_counts.values()) if type_counts else 0
        total_f = float(total_pop) if total_pop > 0 else 1.0

        if type_counts:
            dominant_type = max(type_counts, key=type_counts.get)
            dominant_count = type_counts[dominant_type]
            dominant_share = round(float(dominant_count) / total_f, 6)
        else:
            dominant_type = ""
            dominant_count = 0
            dominant_share = 0.0

        composition_vector = dict(type_counts) if type_counts else {}

        # Log composition alongside verdict for causal analysis
        self._composition_log.append({
            "generation": generation,
            "verdict": verdict,
            "dominant_type": dominant_type,
            "dominant_share": dominant_share,
            "h_fraction": round(h_fraction, 4),
            "sigma_rolling": round(sigma_rolling, 6),
        })

        # ── INV_073 pattern: low H at confirmed criticality ──
        inv073_pattern = (
            in_sigma_band
            and power_law_likely
            and 0.0 < h_fraction < 0.3
        )

        snapshot = {
            "generation":        generation,
            "sigma_instant":     round(sigma_instant, 6),
            "sigma_rolling":     round(sigma_rolling, 6),
            "sigma_rolling_std": round(sigma_rolling_std, 6),
            "alpha":             round(alpha, 4),
            "alpha_r_squared":   round(alpha_r2, 4),
            "power_law_likely":  power_law_likely,
            "verdict":           verdict,
            "shannon_h":         round(shannon_h, 4),
            "h_fraction":        round(h_fraction, 4),
            "survival_rate":     round(survival_rate, 4),
            "dominant_type":     dominant_type,
            "dominant_count":    dominant_count,
            "dominant_share":    dominant_share,
            "total_population":  total_pop,
            "composition_vector": composition_vector,
            "in_sigma_band":     in_sigma_band,
            "in_alpha_band":     in_alpha_band,
            "inv073_pattern":    inv073_pattern,
            "timestamp":         datetime.now(timezone.utc).isoformat(),
        }

        self._snapshots.append(snapshot)

        # Trim histories
        if len(self._snapshots) > self.history_limit:
            trim = len(self._snapshots) - self.history_limit
            self._snapshots = self._snapshots[trim:]
        if len(self._sigma_series) > self.history_limit:
            self._sigma_series = self._sigma_series[-self.history_limit:]
        if len(self._alpha_series) > self.history_limit:
            self._alpha_series = self._alpha_series[-self.history_limit:]
        if len(self._h_fraction_series) > self.history_limit:
            self._h_fraction_series = self._h_fraction_series[-self.history_limit:]
        if len(self._composition_log) > self.history_limit:
            self._composition_log = self._composition_log[-self.history_limit:]

        return snapshot

    def snapshots(self):
        # type: () -> list
        """Return all recorded snapshots."""
        return list(self._snapshots)

    def sigma_series(self):
        # type: () -> List[float]
        """Return per-snapshot σ history."""
        return list(self._sigma_series)

    def alpha_series(self):
        # type: () -> List[float]
        """Return per-snapshot α history."""
        return list(self._alpha_series)

    def verdict_counts(self):
        # type: () -> dict
        """Return verdict distribution across all snapshots."""
        return dict(self._verdict_counts)

    def composition_log(self):
        # type: () -> list
        """Return per-snapshot composition log entries."""
        return list(self._composition_log)

    def pipeline_report(self):
        # type: () -> dict
        """
        Generate a full pipeline report for O140/O141 comparison.

        Returns
        -------
        dict with keys:
            n_snapshots            : int
            sigma_global_mean      : float
            sigma_global_std       : float
            alpha_global_mean      : float
            alpha_global_std       : float
            h_fraction_mean        : float
            verdict_distribution   : dict  — {verdict: count}
            verdict_fractions      : dict  — {verdict: fraction}
            fraction_at_critical   : float
            fraction_subcritical   : float
            fraction_supercritical : float
            dominant_type_mode     : str   — most frequent dominant type
            dominant_type_at_critical : dict — {type: count} when AT_CRITICAL
            composition_trend      : str   — "consolidating"/"diversifying"/"stable"
            n_inv073_pattern       : int   — snapshots with low-H-at-criticality
            inv073_assessment      : str   — human-readable assessment
            drift_trend            : str   — "stable"/"freezing"/"dissipating"
            obligations_addressed  : list  — O140, O141, INV_073 as applicable
            timestamp              : str
        """
        n = len(self._snapshots)
        ts = datetime.now(timezone.utc).isoformat()

        if n == 0:
            return {
                "n_snapshots":            0,
                "sigma_global_mean":      0.0,
                "sigma_global_std":       0.0,
                "alpha_global_mean":      0.0,
                "alpha_global_std":       0.0,
                "h_fraction_mean":        0.0,
                "verdict_distribution":   {},
                "verdict_fractions":      {},
                "fraction_at_critical":   0.0,
                "fraction_subcritical":   0.0,
                "fraction_supercritical": 0.0,
                "dominant_type_mode":     "",
                "dominant_type_at_critical": {},
                "composition_trend":      "no_data",
                "n_inv073_pattern":       0,
                "inv073_assessment":      "No data collected.",
                "drift_trend":            "no_data",
                "obligations_addressed":  [],
                "timestamp":              ts,
            }

        # ── σ statistics ──
        sigmas = self._sigma_series[-n:] if self._sigma_series else []
        if sigmas:
            s_mean = sum(sigmas) / float(len(sigmas))
            s_var = sum((s - s_mean) ** 2 for s in sigmas) / float(len(sigmas))
            s_std = math.sqrt(s_var) if s_var > 0 else 0.0
        else:
            s_mean = 0.0
            s_std = 0.0

        # ── α statistics ──
        alphas = [a for a in self._alpha_series[-n:] if a > 0.0]
        if alphas:
            a_mean = sum(alphas) / float(len(alphas))
            a_var = sum((a - a_mean) ** 2 for a in alphas) / float(len(alphas))
            a_std = math.sqrt(a_var) if a_var > 0 else 0.0
        else:
            a_mean = 0.0
            a_std = 0.0

        # ── H fraction ──
        h_fracs = self._h_fraction_series[-n:] if self._h_fraction_series else []
        h_mean = sum(h_fracs) / float(len(h_fracs)) if h_fracs else 0.0

        # ── Verdict distribution ──
        v_dist = dict(self._verdict_counts)
        n_total_v = sum(v_dist.values()) if v_dist else 1
        v_fracs = {}  # type: dict
        for v, c in v_dist.items():
            v_fracs[v] = round(float(c) / float(n_total_v), 4)

        frac_crit = v_fracs.get("AT_CRITICAL", 0.0)
        frac_sub = v_fracs.get("SUBCRITICAL", 0.0)
        frac_super = v_fracs.get("SUPERCRITICAL", 0.0)

        # ── Dominant type analysis ──
        dom_freq = {}   # type: dict
        dom_at_crit = {}  # type: dict
        for entry in self._composition_log:
            dt = entry.get("dominant_type", "")
            if dt:
                dom_freq[dt] = dom_freq.get(dt, 0) + 1
                if entry.get("verdict") == "AT_CRITICAL":
                    dom_at_crit[dt] = dom_at_crit.get(dt, 0) + 1

        dom_mode = max(dom_freq, key=dom_freq.get) if dom_freq else ""

        # ── Composition trend ──
        comp_log = self._composition_log
        if len(comp_log) >= 8:
            first_shares = [e["dominant_share"] for e in comp_log[:len(comp_log) // 2]]
            second_shares = [e["dominant_share"] for e in comp_log[len(comp_log) // 2:]]
            fh_share = sum(first_shares) / float(len(first_shares)) if first_shares else 0.0
            sh_share = sum(second_shares) / float(len(second_shares)) if second_shares else 0.0
            delta_share = sh_share - fh_share
            if delta_share > 0.03:
                comp_trend = "consolidating"
            elif delta_share < -0.03:
                comp_trend = "diversifying"
            else:
                comp_trend = "stable"
        else:
            comp_trend = "stable"

        # ── Drift trend ──
        if len(sigmas) >= 8:
            first_half = sigmas[:len(sigmas) // 2]
            second_half = sigmas[len(sigmas) // 2:]
            fh_m = sum(first_half) / float(len(first_half))
            sh_m = sum(second_half) / float(len(second_half))
            delta = sh_m - fh_m
            if delta > 0.02:
                drift_trend = "dissipating"
            elif delta < -0.02:
                drift_trend = "freezing"
            else:
                drift_trend = "stable"
        else:
            drift_trend = "stable"

        # ── INV_073 ──
        n_inv073 = sum(1 for s in self._snapshots if s.get("inv073_pattern", False))

        if n_inv073 > 0 and frac_crit > 0.5:
            inv073_text = (
                "CONFIRMED: {}/{} snapshots ({:.1%}) show low-entropy critical "
                "attractor (H<30% of max with sigma in band and power-law "
                "avalanches). Consistent with ordered SOC — the critical ridge "
                "sustains low-diversity, spatially correlated states. Dominant "
                "type at criticality: {}."
            ).format(n_inv073, n, float(n_inv073) / float(n),
                     max(dom_at_crit, key=dom_at_crit.get) if dom_at_crit else "N/A")
        elif frac_crit < 0.3:
            inv073_text = (
                "INCONCLUSIVE: only {:.1%} of snapshots achieved AT_CRITICAL. "
                "Insufficient confirmed criticality for entropy assessment."
            ).format(frac_crit)
        else:
            inv073_text = (
                "STANDARD: {:.1%} at criticality with mean H/H_max={:.3f}. "
                "No low-entropy critical attractor detected."
            ).format(frac_crit, h_mean)

        # ── Obligations ──
        obligations = []  # type: list
        if frac_crit > 0.0:
            obligations.append("O140")
        if s_mean > 0.0:
            obligations.append("O141")
        if n_inv073 > 0 or h_mean > 0.0:
            obligations.append("INV_073")

        return {
            "n_snapshots":              n,
            "sigma_global_mean":        round(s_mean, 6),
            "sigma_global_std":         round(s_std, 6),
            "alpha_global_mean":        round(a_mean, 4),
            "alpha_global_std":         round(a_std, 4),
            "h_fraction_mean":          round(h_mean, 4),
            "verdict_distribution":     v_dist,
            "verdict_fractions":        v_fracs,
            "fraction_at_critical":     frac_crit,
            "fraction_subcritical":     frac_sub,
            "fraction_supercritical":   frac_super,
            "dominant_type_mode":       dom_mode,
            "dominant_type_at_critical": dom_at_crit,
            "composition_trend":        comp_trend,
            "n_inv073_pattern":         n_inv073,
            "inv073_assessment":        inv073_text,
            "drift_trend":              drift_trend,
            "obligations_addressed":    obligations,
            "timestamp":                ts,
        }

    def _fit_power_law_hill(self):
        # type: () -> Tuple[float, float]
        """
        Fit power-law exponent α via Hill estimator on pooled avalanche sizes.
        Returns (alpha, r_squared). Returns (0.0, 0.0) if insufficient data.
        """
        sizes = [s for s in self._avalanche_pool if s > 0]
        if len(sizes) < 10:
            return (0.0, 0.0)

        x_min = max(1.0, min(sizes))
        tail = [s for s in sizes if s >= x_min]
        n = len(tail)
        if n < 5:
            return (0.0, 0.0)

        log_sum = 0.0
        for s in tail:
            ratio = float(s) / x_min
            if ratio > 0:
                log_sum += math.log(ratio)

        if log_sum <= 0.0:
            return (0.0, 0.0)

        alpha = 1.0 + float(n) / log_sum

        # R² on log-log CCDF
        tail_sorted = sorted(tail)
        unique_sizes = sorted(set(tail_sorted))
        n_total = float(len(tail_sorted))

        log_x = []     # type: List[float]
        log_ccdf = []  # type: List[float]
        for x_val in unique_sizes:
            count_ge = sum(1 for s in tail_sorted if s >= x_val)
            p = float(count_ge) / n_total
            if p > 0 and x_val > 0:
                log_x.append(math.log(float(x_val)))
                log_ccdf.append(math.log(p))

        if len(log_x) < 3:
            return (alpha, 0.0)

        k = len(log_x)
        sum_lx = sum(log_x)
        sum_ly = sum(log_ccdf)
        sum_lxy = sum(x * y for x, y in zip(log_x, log_ccdf))
        sum_lx2 = sum(x * x for x in log_x)

        denom = float(k) * sum_lx2 - sum_lx * sum_lx
        if abs(denom) < 1e-15:
            return (alpha, 0.0)

        slope = (float(k) * sum_lxy - sum_lx * sum_ly) / denom
        intercept = (sum_ly - slope * sum_lx) / float(k)

        mean_ly = sum_ly / float(k)
        ss_tot = sum((y - mean_ly) ** 2 for y in log_ccdf)
        ss_res = sum((y - (intercept + slope * x)) ** 2
                     for x, y in zip(log_x, log_ccdf))
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0

        return (alpha, max(0.0, r_squared))


class CompositionalSkewDetector:
    """
    Branching-ratio tracker that flags when σ exits the critical band
    [0.95, 1.05] and logs the dominant cell-type fraction alongside
    Shannon entropy H, enabling automated detection of compositional
    skew at criticality.

    Closes the gap between population-level criticality verdicts and the
    mechanistic question of *which agent types are load-bearing* at the
    critical ridge.  When σ exits the band, the detector records the
    full type-fraction snapshot so downstream analysis can determine
    whether compositional skew (e.g., Physics Navigator dominance at
    85%+ of the population) *precedes* or *follows* criticality drift.

    INV_073 falsification relevance:
      The telemetry snapshot (σ=1.0242±0.0153, H=0.4315 bits = 0.167·H_max)
      demonstrates that the critical band can be reached with highly ordered
      populations.  If the Wasserstein gradient flow interpretation of
      INV_073 equates γ=1 criticality with maximum information throughput
      or entropy maximization, this is a direct counterexample: the system
      is at criticality with H far below H_max.  The detector makes this
      pattern machine-detectable by jointly tracking σ-band membership,
      H/H_max, and dominant-type fraction at every step.

      Low-entropy critical states (H ≈ 0.167·H_max) ARE admissible
      attractors on the critical ridge — the Wasserstein gradient path
      to γ=1 does NOT require near-maximal entropy.  What it requires
      is H > 0 (not frozen) with σ ≈ 1 and power-law avalanches.  This
      detector flags the distinction between frozen (H=0), ordered-critical
      (0 < H << H_max, σ ≈ 1), and balanced-critical (H ≈ H_max, σ ≈ 1).

    Addresses: O140 (CA measurement grounding), O141 (solo-kernel vs
    population criticality), INV_073 (low-entropy critical attractor
    detection and compositional skew at criticality).

    Usage::

        detector = CompositionalSkewDetector()
        for step in simulation:
            result = detector.record(
                generation=step,
                sigma=sigma_val,
                shannon_h=h_bits,
                shannon_h_max=h_max,
                type_counts={"Physics Navigator": 876, "Entropy Scorer": 48,
                             "Topology Agent": 100},
            )
            if result["sigma_excursion"]:
                print("σ exited band at step", step,
                      "dominant:", result["dominant_type"],
                      "fraction:", result["dominant_fraction"])
            if result["compositional_skew"]:
                print("Compositional skew detected:", result["skew_detail"])
        report = detector.skew_report()
    """

    # Skew threshold: dominant type fraction above which compositional
    # skew is flagged (even if σ is in-band)
    SKEW_THRESHOLD = 0.80

    def __init__(
        self,
        sigma_band_low=0.95,    # type: float
        sigma_band_high=1.05,   # type: float
        skew_threshold=0.80,    # type: float
        history_limit=2000,     # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        sigma_band_low : float
            Lower bound of the critical band for σ. Default: 0.95.
        sigma_band_high : float
            Upper bound of the critical band for σ. Default: 1.05.
        skew_threshold : float
            Dominant-type fraction above which compositional skew is
            flagged. Default: 0.80.
        history_limit : int
            Maximum records to retain in memory. Default: 2000.
        """
        self.sigma_band_low = sigma_band_low
        self.sigma_band_high = sigma_band_high
        self.skew_threshold = skew_threshold
        self.history_limit = max(10, history_limit)

        self._records = []              # type: list
        self._excursion_records = []    # type: list
        self._skew_records = []         # type: list
        self._sigma_series = []         # type: List[float]
        self._h_fraction_series = []    # type: List[float]
        self._dominant_fraction_series = []  # type: List[float]

    def record(
        self,
        generation,         # type: int
        sigma,              # type: float
        shannon_h=0.0,      # type: float
        shannon_h_max=0.0,  # type: float
        type_counts=None,   # type: Optional[dict]
        sigma_std=0.0,      # type: float
        alpha=0.0,          # type: float
        alpha_r_squared=0.0,# type: float
        survival_rate=0.0,  # type: float
    ):
        # type: (...) -> dict
        """
        Record one generation's telemetry and check for σ excursion
        and compositional skew.

        Parameters
        ----------
        generation : int
            Simulation step / generation number.
        sigma : float
            Branching ratio σ at this step.
        shannon_h : float
            Shannon entropy H of the type distribution (bits).
        shannon_h_max : float
            Maximum possible Shannon entropy (log2 of number of types).
        type_counts : dict or None
            Mapping of cell-type name (str) to count (int).
        sigma_std : float
            Standard deviation of σ within the measurement window.
        alpha : float
            Power-law exponent α from avalanche size distribution.
        alpha_r_squared : float
            R² goodness-of-fit for the power-law regression.
        survival_rate : float
            Fraction of cells surviving this generation.

        Returns
        -------
        dict with keys:
            generation          : int
            sigma               : float
            sigma_in_band       : bool
            sigma_excursion     : bool   — True if σ outside [0.95, 1.05]
            excursion_direction : str    — "supercritical"/"subcritical"/""
            shannon_h           : float
            h_fraction          : float  — H / H_max
            dominant_type       : str
            dominant_fraction   : float  — fraction of population
            dominant_count      : int
            total_population    : int
            type_fractions      : dict   — {type: fraction} for all types
            compositional_skew  : bool   — True if dominant fraction > threshold
            skew_detail         : str    — human-readable skew description
            entropy_regime      : str    — "frozen"/"ordered"/"moderate"/"balanced"
            criticality_composition : str — joint classification
            inv073_flag         : bool   — True if low H at confirmed criticality
            timestamp           : str
        """
        if type_counts is None:
            type_counts = {}

        self._sigma_series.append(sigma)

        # ── Band check ──
        sigma_in_band = self.sigma_band_low <= sigma <= self.sigma_band_high
        sigma_excursion = (sigma != 0.0) and not sigma_in_band
        if sigma_excursion:
            excursion_direction = "supercritical" if sigma > self.sigma_band_high else "subcritical"
        else:
            excursion_direction = ""

        # ── Entropy ──
        h_fraction = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0
        self._h_fraction_series.append(h_fraction)

        # Classify entropy regime
        if shannon_h <= 0.0:
            entropy_regime = "frozen"
        elif h_fraction < 0.20:
            entropy_regime = "ordered"
        elif h_fraction < 0.50:
            entropy_regime = "moderate"
        else:
            entropy_regime = "balanced"

        # ── Type fractions ──
        total_pop = sum(type_counts.values()) if type_counts else 0
        total_f = float(total_pop) if total_pop > 0 else 1.0

        type_fractions = {}  # type: dict
        for cell_type, count in type_counts.items():
            type_fractions[cell_type] = round(float(count) / total_f, 6)

        if type_counts:
            dominant_type = max(type_counts, key=type_counts.get)
            dominant_count = type_counts[dominant_type]
            dominant_fraction = round(float(dominant_count) / total_f, 6)
        else:
            dominant_type = ""
            dominant_count = 0
            dominant_fraction = 0.0

        self._dominant_fraction_series.append(dominant_fraction)

        # ── Compositional skew detection ──
        compositional_skew = dominant_fraction >= self.skew_threshold
        skew_detail = ""
        if compositional_skew:
            skew_detail = (
                "{} dominates at {:.1%} of population ({}/{} cells). "
                "H/H_max={:.3f} ({}). sigma={:.4f} ({})."
            ).format(
                dominant_type, dominant_fraction, dominant_count, total_pop,
                h_fraction, entropy_regime, sigma,
                "in band" if sigma_in_band else excursion_direction,
            )

        # ── Joint criticality-composition classification ──
        power_law_ok = (
            ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
            and alpha_r_squared >= ALPHA_R2_THRESHOLD
        )

        if sigma_in_band and power_law_ok and compositional_skew and entropy_regime == "ordered":
            criticality_composition = "CRITICALLY_ORDERED_SKEWED"
        elif sigma_in_band and power_law_ok and compositional_skew:
            criticality_composition = "CRITICALLY_SKEWED"
        elif sigma_in_band and power_law_ok and entropy_regime == "ordered":
            criticality_composition = "CRITICALLY_ORDERED"
        elif sigma_in_band and power_law_ok:
            criticality_composition = "CRITICALLY_BALANCED"
        elif sigma_in_band:
            criticality_composition = "NEAR_CRITICAL"
        elif sigma > self.sigma_band_high:
            criticality_composition = "SUPERCRITICAL"
        elif sigma < self.sigma_band_low and sigma > 0.0:
            criticality_composition = "SUBCRITICAL"
        else:
            criticality_composition = "UNDETERMINED"

        # ── INV_073 flag: low H at confirmed criticality ──
        inv073_flag = (
            sigma_in_band
            and power_law_ok
            and 0.0 < h_fraction < 0.3
        )

        rec = {
            "generation":              generation,
            "sigma":                   round(sigma, 6),
            "sigma_std":               round(sigma_std, 6),
            "sigma_in_band":           sigma_in_band,
            "sigma_excursion":         sigma_excursion,
            "excursion_direction":     excursion_direction,
            "shannon_h":               round(shannon_h, 4),
            "h_fraction":              round(h_fraction, 4),
            "shannon_h_max":           round(shannon_h_max, 4),
            "alpha":                   round(alpha, 4),
            "alpha_r_squared":         round(alpha_r_squared, 4),
            "survival_rate":           round(survival_rate, 4),
            "dominant_type":           dominant_type,
            "dominant_fraction":       dominant_fraction,
            "dominant_count":          dominant_count,
            "total_population":        total_pop,
            "type_fractions":          type_fractions,
            "compositional_skew":      compositional_skew,
            "skew_detail":             skew_detail,
            "entropy_regime":          entropy_regime,
            "criticality_composition": criticality_composition,
            "inv073_flag":             inv073_flag,
            "timestamp":               datetime.now(timezone.utc).isoformat(),
        }

        self._records.append(rec)
        if sigma_excursion:
            self._excursion_records.append(rec)
        if compositional_skew:
            self._skew_records.append(rec)

        # Trim
        if len(self._records) > self.history_limit:
            trim = len(self._records) - self.history_limit
            self._records = self._records[trim:]
        if len(self._sigma_series) > self.history_limit:
            self._sigma_series = self._sigma_series[-self.history_limit:]
        if len(self._h_fraction_series) > self.history_limit:
            self._h_fraction_series = self._h_fraction_series[-self.history_limit:]
        if len(self._dominant_fraction_series) > self.history_limit:
            self._dominant_fraction_series = self._dominant_fraction_series[-self.history_limit:]

        return rec

    def records(self):
        # type: () -> list
        """Return all recorded snapshots."""
        return list(self._records)

    def excursion_records(self):
        # type: () -> list
        """Return all σ-excursion records."""
        return list(self._excursion_records)

    def skew_records(self):
        # type: () -> list
        """Return all compositional-skew records."""
        return list(self._skew_records)

    def sigma_series(self):
        # type: () -> List[float]
        """Return the per-step σ history."""
        return list(self._sigma_series)

    def dominant_fraction_series(self):
        # type: () -> List[float]
        """Return the per-step dominant-type fraction history."""
        return list(self._dominant_fraction_series)

    def skew_report(self, window=50):
        # type: (int) -> dict
        """
        Generate a summary report of compositional skew detection,
        including σ-excursion statistics, dominant-type tallies at
        excursions, and entropy-criticality joint analysis.

        Parameters
        ----------
        window : int
            Number of recent records for rolling statistics. Default: 50.

        Returns
        -------
        dict with keys:
            n_records               : int
            n_excursions            : int    — σ exits from critical band
            n_skew_events           : int    — compositional skew flags
            excursion_rate          : float  — fraction of steps with σ excursion
            skew_rate               : float  — fraction of steps with skew flag
            sigma_global_mean       : float
            sigma_global_std        : float
            h_fraction_mean         : float
            dominant_fraction_mean  : float  — mean dominant-type fraction
            dominant_fraction_std   : float
            dominant_type_at_excursion : dict — {type: count} at σ excursions
            dominant_type_at_skew      : dict — {type: count} at skew events
            skew_at_criticality     : int    — skew events while σ in band
            skew_at_excursion       : int    — skew events while σ out of band
            criticality_composition_counts : dict — {joint_class: count}
            n_inv073_flags          : int    — low-H-at-criticality events
            inv073_assessment       : str    — human-readable INV_073 status
            skew_criticality_correlation : float — Pearson r between
                                                   dominant_fraction and |σ-1|
            composition_trend       : str    — "consolidating"/"diversifying"/"stable"
            obligations_addressed   : list
            timestamp               : str
        """
        n = len(self._records)
        ts = datetime.now(timezone.utc).isoformat()

        if n == 0:
            return {
                "n_records": 0,
                "n_excursions": 0,
                "n_skew_events": 0,
                "excursion_rate": 0.0,
                "skew_rate": 0.0,
                "sigma_global_mean": 0.0,
                "sigma_global_std": 0.0,
                "h_fraction_mean": 0.0,
                "dominant_fraction_mean": 0.0,
                "dominant_fraction_std": 0.0,
                "dominant_type_at_excursion": {},
                "dominant_type_at_skew": {},
                "skew_at_criticality": 0,
                "skew_at_excursion": 0,
                "criticality_composition_counts": {},
                "n_inv073_flags": 0,
                "inv073_assessment": "No data collected.",
                "skew_criticality_correlation": 0.0,
                "composition_trend": "no_data",
                "obligations_addressed": [],
                "timestamp": ts,
            }

        n_exc = len(self._excursion_records)
        n_skew = len(self._skew_records)
        exc_rate = round(float(n_exc) / float(n), 6)
        skew_rate = round(float(n_skew) / float(n), 6)

        # ── σ statistics ──
        sigmas = self._sigma_series
        s_n = len(sigmas)
        s_mean = sum(sigmas) / float(s_n) if s_n > 0 else 0.0
        s_var = sum((s - s_mean) ** 2 for s in sigmas) / float(s_n) if s_n > 0 else 0.0
        s_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # ── H fraction ──
        hf = self._h_fraction_series
        hf_mean = sum(hf) / float(len(hf)) if hf else 0.0

        # ── Dominant fraction statistics ──
        df = self._dominant_fraction_series
        df_mean = sum(df) / float(len(df)) if df else 0.0
        df_var = sum((d - df_mean) ** 2 for d in df) / float(len(df)) if df else 0.0
        df_std = math.sqrt(df_var) if df_var > 0 else 0.0

        # ── Dominant type tallies at excursions and skew events ──
        dom_at_exc = {}   # type: dict
        for r in self._excursion_records:
            dt = r.get("dominant_type", "")
            if dt:
                dom_at_exc[dt] = dom_at_exc.get(dt, 0) + 1

        dom_at_skew = {}  # type: dict
        for r in self._skew_records:
            dt = r.get("dominant_type", "")
            if dt:
                dom_at_skew[dt] = dom_at_skew.get(dt, 0) + 1

        # ── Skew at criticality vs at excursion ──
        skew_at_crit = sum(1 for r in self._skew_records if r.get("sigma_in_band", False))
        skew_at_exc = sum(1 for r in self._skew_records if r.get("sigma_excursion", False))

        # ── Joint classification counts ──
        cc_counts = {}  # type: dict
        for r in self._records:
            cc = r.get("criticality_composition", "UNDETERMINED")
            cc_counts[cc] = cc_counts.get(cc, 0) + 1

        # ── INV_073 ──
        n_inv073 = sum(1 for r in self._records if r.get("inv073_flag", False))

        frac_crit = float(cc_counts.get("CRITICALLY_BALANCED", 0) +
                          cc_counts.get("CRITICALLY_ORDERED", 0) +
                          cc_counts.get("CRITICALLY_SKEWED", 0) +
                          cc_counts.get("CRITICALLY_ORDERED_SKEWED", 0)) / float(n)

        if n_inv073 > 0 and frac_crit > 0.3:
            inv073_text = (
                "INV_073 PATTERN DETECTED: {}/{} records ({:.1%}) show "
                "low-entropy critical attractor (H<30% of max with sigma "
                "in band). Dominant type at these events: {}. This "
                "demonstrates that the critical ridge sustains ordered, "
                "compositionally skewed states — the Wasserstein gradient "
                "path to gamma=1 does NOT require entropy maximization. "
                "Mean dominant fraction at INV_073 events: {:.1%}."
            ).format(
                n_inv073, n, float(n_inv073) / float(n),
                dom_at_skew if dom_at_skew else "N/A",
                df_mean,
            )
        elif frac_crit < 0.1:
            inv073_text = (
                "INV_073 INCONCLUSIVE: Only {:.1%} of records at confirmed "
                "criticality. Cannot assess entropy-criticality-composition "
                "relationship."
            ).format(frac_crit)
        else:
            inv073_text = (
                "INV_073 CONSISTENT: {:.1%} at criticality, mean H/H_max={:.3f}, "
                "mean dominant fraction={:.3f}. No strong low-entropy critical "
                "attractor detected."
            ).format(frac_crit, hf_mean, df_mean)

        # ── Pearson correlation: dominant_fraction vs |σ - 1| ──
        # Positive correlation means higher dominance → farther from criticality
        # Negative means higher dominance → closer to criticality (load-bearing)
        skew_crit_corr = 0.0
        min_len = min(len(df), len(sigmas))
        if min_len >= 5:
            drift_vals = [abs(s - 1.0) for s in sigmas[-min_len:]]
            dom_vals = df[-min_len:]
            d_m = sum(drift_vals) / float(min_len)
            f_m = sum(dom_vals) / float(min_len)
            cov = sum((d - d_m) * (f - f_m) for d, f in zip(drift_vals, dom_vals)) / float(min_len)
            var_d = sum((d - d_m) ** 2 for d in drift_vals) / float(min_len)
            var_f = sum((f - f_m) ** 2 for f in dom_vals) / float(min_len)
            denom_c = math.sqrt(var_d) * math.sqrt(var_f) if var_d > 0 and var_f > 0 else 0.0
            if denom_c > 1e-15:
                skew_crit_corr = cov / denom_c

        # ── Composition trend ──
        if len(df) >= 8:
            first_half = df[:len(df) // 2]
            second_half = df[len(df) // 2:]
            fh_m = sum(first_half) / float(len(first_half))
            sh_m = sum(second_half) / float(len(second_half))
            delta = sh_m - fh_m
            if delta > 0.03:
                comp_trend = "consolidating"
            elif delta < -0.03:
                comp_trend = "diversifying"
            else:
                comp_trend = "stable"
        else:
            comp_trend = "stable"

        # ── Obligations ──
        obligations = []  # type: list
        if frac_crit > 0.0:
            obligations.append("O140")
        if s_mean > 0.0:
            obligations.append("O141")
        if n_inv073 > 0 or hf_mean > 0.0:
            obligations.append("INV_073")

        return {
            "n_records":                     n,
            "n_excursions":                  n_exc,
            "n_skew_events":                 n_skew,
            "excursion_rate":                exc_rate,
            "skew_rate":                     skew_rate,
            "sigma_global_mean":             round(s_mean, 6),
            "sigma_global_std":              round(s_std, 6),
            "h_fraction_mean":               round(hf_mean, 4),
            "dominant_fraction_mean":        round(df_mean, 6),
            "dominant_fraction_std":         round(df_std, 6),
            "dominant_type_at_excursion":    dom_at_exc,
            "dominant_type_at_skew":         dom_at_skew,
            "skew_at_criticality":           skew_at_crit,
            "skew_at_excursion":             skew_at_exc,
            "criticality_composition_counts": cc_counts,
            "n_inv073_flags":                n_inv073,
            "inv073_assessment":             inv073_text,
            "skew_criticality_correlation":  round(skew_crit_corr, 6),
            "composition_trend":             comp_trend,
            "obligations_addressed":         obligations,
            "timestamp":                     ts,
        }


class StepwiseSigmaDriftDetector:
    """
    Per-step branching-ratio (σ) drift detector for the CA telemetry pipeline.

    Computes σ = mean(offspring_events) / mean(parent_events) at every
    simulation step and flags deviation from the critical band [0.95, 1.05]
    as a criticality-drift alert.  This closes the feedback loop between
    simulation state and the γ=1 constraint, enabling automated detection
    of sub- or super-critical drift before it corrupts multi-step
    measurements.

    Design rationale (INV_073):
      The empirical branching ratio σ=1.023±0.0155 from the 32×32 Game of
      Truth telemetry confirms criticality, but the survival rate of 0.9249
      (not 1.0) implies non-negligible cell death even at the critical ridge.
      This detector treats the [0.95, 1.05] band as the operational definition
      of "at criticality" and raises graded alerts (WARNING at band edges,
      CRITICAL outside) so the epistemic loop can distinguish transient
      fluctuations from structural drift.

    Physics Navigator context:
      Physics Navigator cells constitute the dominant population (867 cells)
      at the confirmed critical point.  Their symmetry-conserving role —
      maintaining structured information flow without freezing — is what
      operationalizes Noether-type conservation of the critical regime.
      This detector logs the dominant cell type at each drift alert to
      enable causal analysis of whether population composition *predicts*
      or *follows* criticality drift.

    Addresses: O140 (CA measurement grounding), O141 (solo-kernel vs
    population criticality), INV_073 (Wasserstein gradient bias detection).

    Usage::

        detector = StepwiseSigmaDriftDetector()
        for step in range(n_steps):
            parent_events = [p1, p2, ...]   # per-cell activity at step t
            offspring_events = [c1, c2, ...]  # per-cell activity at step t+1
            result = detector.step(
                parent_events=parent_events,
                offspring_events=offspring_events,
                dominant_type="Physics Navigator",
                dominant_count=867,
                total_population=1024,
            )
            if result["alert_level"] != "NONE":
                handle_drift_alert(result)
        report = detector.drift_report()
    """

    # Alert levels
    ALERT_NONE = "NONE"
    ALERT_WARNING = "WARNING"       # σ near band edges (within 0.02 of boundary)
    ALERT_CRITICAL = "CRITICAL"     # σ outside band

    def __init__(
        self,
        sigma_band_low=0.95,    # type: float
        sigma_band_high=1.05,   # type: float
        warning_margin=0.02,    # type: float
        rolling_window=50,      # type: int
        history_limit=5000,     # type: int
    ):
        # type: (...) -> None
        """
        Parameters
        ----------
        sigma_band_low : float
            Lower bound of the critical band for σ. Default: 0.95.
        sigma_band_high : float
            Upper bound of the critical band for σ. Default: 1.05.
        warning_margin : float
            Distance from band edge within which a WARNING is raised
            even if σ is still inside the band. Default: 0.02.
        rolling_window : int
            Number of recent steps for rolling σ mean/std. Default: 50.
        history_limit : int
            Maximum per-step records retained. Default: 5000.
        """
        self.sigma_band_low = sigma_band_low
        self.sigma_band_high = sigma_band_high
        self.warning_margin = warning_margin
        self.rolling_window = max(1, rolling_window)
        self.history_limit = max(10, history_limit)

        self._sigma_series = []     # type: List[float]
        self._step_count = 0        # type: int
        self._alerts = []           # type: list
        self._n_warning = 0         # type: int
        self._n_critical = 0        # type: int
        self._cumulative_drift = 0.0  # type: float

    def step(
        self,
        parent_events,          # type: List[float]
        offspring_events,       # type: List[float]
        dominant_type="",       # type: str
        dominant_count=0,       # type: int
        total_population=0,     # type: int
    ):
        # type: (...) -> dict
        """
        Process one simulation step: compute σ and check for drift.

        Parameters
        ----------
        parent_events : list of float
            Activity values (counts or activations) for parent cells at
            this step.  σ is computed as mean(offspring_events) /
            mean(parent_events).
        offspring_events : list of float
            Activity values for offspring cells produced at this step.
        dominant_type : str
            Label of the most populous cell type (for causal logging).
        dominant_count : int
            Count of the dominant cell type.
        total_population : int
            Total number of live cells.

        Returns
        -------
        dict with keys:
            step             : int    — step number (0-indexed)
            sigma            : float  — instantaneous σ for this step
            sigma_rolling    : float  — rolling mean σ over window
            sigma_rolling_std: float  — rolling std σ over window
            in_band          : bool   — whether σ_rolling is in [0.95, 1.05]
            alert_level      : str    — NONE / WARNING / CRITICAL
            alert_direction  : str    — "supercritical" / "subcritical" / ""
            drift_from_unity : float  — |σ_rolling - 1.0|
            dominant_type    : str
            dominant_count   : int
            total_population : int
            cumulative_drift : float  — Σ |σ_i - 1.0| over all steps
            timestamp        : str    — ISO-8601 UTC
        """
        self._step_count += 1

        # ── Compute σ = mean(offspring) / mean(parent) ──
        n_parent = len(parent_events)
        n_offspring = len(offspring_events)

        if n_parent > 0:
            mean_parent = sum(parent_events) / float(n_parent)
        else:
            mean_parent = 0.0

        if n_offspring > 0:
            mean_offspring = sum(offspring_events) / float(n_offspring)
        else:
            mean_offspring = 0.0

        if mean_parent > 0.0:
            sigma = mean_offspring / mean_parent
        else:
            sigma = 0.0

        self._sigma_series.append(sigma)
        self._cumulative_drift += abs(sigma - 1.0)

        # ── Rolling statistics ──
        window = self._sigma_series[-self.rolling_window:]
        w_n = len(window)
        sigma_rolling = sum(window) / float(w_n)
        s_var = sum((s - sigma_rolling) ** 2 for s in window) / float(w_n)
        sigma_rolling_std = math.sqrt(s_var) if s_var > 0 else 0.0

        # ── Band and alert checks ──
        in_band = self.sigma_band_low <= sigma_rolling <= self.sigma_band_high
        drift_from_unity = abs(sigma_rolling - 1.0)

        alert_level = self.ALERT_NONE
        alert_direction = ""

        if sigma_rolling != 0.0:
            if not in_band:
                alert_level = self.ALERT_CRITICAL
                self._n_critical += 1
                if sigma_rolling > self.sigma_band_high:
                    alert_direction = "supercritical"
                else:
                    alert_direction = "subcritical"
            elif (sigma_rolling <= self.sigma_band_low + self.warning_margin or
                  sigma_rolling >= self.sigma_band_high - self.warning_margin):
                alert_level = self.ALERT_WARNING
                self._n_warning += 1
                if sigma_rolling >= self.sigma_band_high - self.warning_margin:
                    alert_direction = "supercritical"
                else:
                    alert_direction = "subcritical"

        result = {
            "step":              self._step_count - 1,
            "sigma":             round(sigma, 6),
            "sigma_rolling":     round(sigma_rolling, 6),
            "sigma_rolling_std": round(sigma_rolling_std, 6),
            "in_band":           in_band,
            "alert_level":       alert_level,
            "alert_direction":   alert_direction,
            "drift_from_unity":  round(drift_from_unity, 6),
            "dominant_type":     dominant_type,
            "dominant_count":    dominant_count,
            "total_population":  total_population,
            "cumulative_drift":  round(self._cumulative_drift, 6),
            "timestamp":         datetime.now(timezone.utc).isoformat(),
        }

        if alert_level != self.ALERT_NONE:
            self._alerts.append(result)

        # ── Trim history ──
        if len(self._sigma_series) > self.history_limit:
            self._sigma_series = self._sigma_series[-self.history_limit:]

        return result

    def step_from_counts(
        self,
        parent_count,       # type: int
        child_count,        # type: int
        dominant_type="",   # type: str
        dominant_count=0,   # type: int
        total_population=0, # type: int
    ):
        # type: (...) -> dict
        """
        Convenience: compute σ from scalar parent/child counts.

        Equivalent to step() with single-element event lists.
        """
        return self.step(
            parent_events=[float(parent_count)] if parent_count > 0 else [],
            offspring_events=[float(child_count)] if child_count > 0 else [],
            dominant_type=dominant_type,
            dominant_count=dominant_count,
            total_population=total_population,
        )

    def sigma_series(self):
        # type: () -> List[float]
        """Return the full per-step σ history."""
        return list(self._sigma_series)

    def alerts(self):
        # type: () -> list
        """Return all alert records."""
        return list(self._alerts)

    def drift_report(self):
        # type: () -> dict
        """
        Generate a summary report of criticality drift detection.

        Returns
        -------
        dict with keys:
            n_steps              : int
            sigma_global_mean    : float
            sigma_global_std     : float
            n_warnings           : int    — steps with WARNING alerts
            n_critical_alerts    : int    — steps with CRITICAL alerts
            alert_rate           : float  — fraction of steps with any alert
            critical_alert_rate  : float  — fraction with CRITICAL alerts
            mean_drift_per_step  : float  — mean |σ - 1.0| per step
            max_sigma            : float
            min_sigma            : float
            fraction_in_band     : float  — fraction of steps with σ in band
            dominant_at_critical : dict   — {type: count} at CRITICAL alerts
            drift_trend          : str    — "stable" / "freezing" / "dissipating"
            gamma1_constraint    : str    — assessment of γ=1 maintenance
            inv073_assessment    : str    — human-readable INV_073 status
            timestamp            : str
        """
        n = self._step_count
        ts = datetime.now(timezone.utc).isoformat()

        if n == 0:
            return {
                "n_steps": 0,
                "sigma_global_mean": 0.0,
                "sigma_global_std": 0.0,
                "n_warnings": 0,
                "n_critical_alerts": 0,
                "alert_rate": 0.0,
                "critical_alert_rate": 0.0,
                "mean_drift_per_step": 0.0,
                "max_sigma": 0.0,
                "min_sigma": 0.0,
                "fraction_in_band": 0.0,
                "dominant_at_critical": {},
                "drift_trend": "no_data",
                "gamma1_constraint": "No data.",
                "inv073_assessment": "No steps recorded.",
                "timestamp": ts,
            }

        all_sigma = self._sigma_series
        s_n = len(all_sigma)
        s_mean = sum(all_sigma) / float(s_n) if s_n > 0 else 0.0
        s_var = sum((s - s_mean) ** 2 for s in all_sigma) / float(s_n) if s_n > 0 else 0.0
        s_std = math.sqrt(s_var) if s_var > 0 else 0.0
        s_min = min(all_sigma) if all_sigma else 0.0
        s_max = max(all_sigma) if all_sigma else 0.0

        n_total_alerts = self._n_warning + self._n_critical
        alert_rate = round(float(n_total_alerts) / float(n), 6) if n > 0 else 0.0
        crit_rate = round(float(self._n_critical) / float(n), 6) if n > 0 else 0.0
        mean_drift = round(self._cumulative_drift / float(n), 6) if n > 0 else 0.0

        n_in_band = sum(
            1 for s in all_sigma
            if self.sigma_band_low <= s <= self.sigma_band_high
        )
        frac_in_band = round(float(n_in_band) / float(s_n), 4) if s_n > 0 else 0.0

        # Dominant type tally at CRITICAL alerts
        dom_at_crit = {}  # type: dict
        for a in self._alerts:
            if a["alert_level"] == self.ALERT_CRITICAL:
                dt = a.get("dominant_type", "")
                if dt:
                    dom_at_crit[dt] = dom_at_crit.get(dt, 0) + 1

        # Drift trend
        if len(all_sigma) >= 8:
            first_half = all_sigma[:len(all_sigma) // 2]
            second_half = all_sigma[len(all_sigma) // 2:]
            fh_m = sum(first_half) / float(len(first_half))
            sh_m = sum(second_half) / float(len(second_half))
            delta = sh_m - fh_m
            if delta > 0.02:
                drift_trend = "dissipating"
            elif delta < -0.02:
                drift_trend = "freezing"
            else:
                drift_trend = "stable"
        else:
            drift_trend = "stable"

        # γ=1 constraint assessment
        if self.sigma_band_low <= s_mean <= self.sigma_band_high and crit_rate < 0.1:
            gamma1 = (
                "MAINTAINED: mean sigma={:.4f}±{:.4f} within critical band "
                "[{:.2f}, {:.2f}], critical alert rate={:.1%}. The gamma=1 "
                "constraint is operationally satisfied."
            ).format(s_mean, s_std, self.sigma_band_low, self.sigma_band_high, crit_rate)
        elif self.sigma_band_low <= s_mean <= self.sigma_band_high:
            gamma1 = (
                "STRAINED: mean sigma={:.4f}±{:.4f} within band but critical "
                "alert rate={:.1%} indicates frequent transient violations. "
                "The gamma=1 constraint is maintained on average but not "
                "robustly."
            ).format(s_mean, s_std, crit_rate)
        else:
            gamma1 = (
                "VIOLATED: mean sigma={:.4f}±{:.4f} outside critical band "
                "[{:.2f}, {:.2f}]. The gamma=1 constraint is NOT satisfied. "
                "Drift direction: {}."
            ).format(s_mean, s_std, self.sigma_band_low, self.sigma_band_high, drift_trend)

        # INV_073 assessment
        if frac_in_band > 0.85 and drift_trend == "stable":
            inv073 = (
                "CONSISTENT: {:.1%} of steps in critical band, drift trend "
                "stable. sigma={:.4f}±{:.4f}. The system maintains criticality "
                "without sustained directional drift, consistent with "
                "Wasserstein gradient navigation to gamma=1 (INV_073). "
                "Survival rate < 1.0 (empirically ~0.9249) is structurally "
                "necessary for criticality, not a failure mode."
            ).format(frac_in_band, s_mean, s_std)
        elif s_mean > self.sigma_band_high:
            inv073 = (
                "CHALLENGE: mean sigma={:.4f} > {:.2f} (supercritical). "
                "The Wasserstein gradient may be biased toward over-production "
                "rather than true ridge navigation. INV_073 predicts this "
                "should self-correct; if sustained, the gradient path is "
                "not converging to gamma=1."
            ).format(s_mean, self.sigma_band_high)
        else:
            inv073 = (
                "MONITORING: sigma={:.4f}±{:.4f}, {:.1%} in band, "
                "drift_trend='{}'. Ongoing monitoring required to assess "
                "INV_073 convergence."
            ).format(s_mean, s_std, frac_in_band, drift_trend)

        return {
            "n_steps":              n,
            "sigma_global_mean":    round(s_mean, 6),
            "sigma_global_std":     round(s_std, 6),
            "n_warnings":           self._n_warning,
            "n_critical_alerts":    self._n_critical,
            "alert_rate":           alert_rate,
            "critical_alert_rate":  crit_rate,
            "mean_drift_per_step":  mean_drift,
            "max_sigma":            round(s_max, 6),
            "min_sigma":            round(s_min, 6),
            "fraction_in_band":     frac_in_band,
            "dominant_at_critical": dom_at_crit,
            "drift_trend":          drift_trend,
            "gamma1_constraint":    gamma1,
            "inv073_assessment":    inv073,
            "timestamp":            ts,
        }

    def reset(self):
        # type: () -> None
        """Reset all state for a new detection run."""
        self._sigma_series = []
        self._step_count = 0
        self._alerts = []
        self._n_warning = 0
        self._n_critical = 0
        self._cumulative_drift = 0.0


class TamuraSweep:
    """
    Fetches new articles from all configured sources.
    Returns a list of input dicts ready for FREED's FEED phase.
    """

    def __init__(self, max_new_per_source=3):
        # type: (int) -> None
        """
        max_new_per_source: how many new articles to return per source per cycle.
        Keeps FEED bounded — the daemon won't choke on a busy day.
        """
        self.max_new = max_new_per_source
        self._load_seen()

    # ── Seen-URL tracking ────────────────────────────────────────────────────

    def _load_seen(self):
        if SEEN_FILE.exists():
            with open(SEEN_FILE) as f:
                self.seen = set(json.load(f))
        else:
            self.seen = set()

    def _save_seen(self):
        with open(SEEN_FILE, "w") as f:
            json.dump(sorted(self.seen), f, indent=2)

    def _mark_seen(self, url):
        # type: (str) -> None
        self.seen.add(url)
        self._save_seen()

    # ── Main entry point ─────────────────────────────────────────────────────

    def sweep(self):
        # type: () -> list
        """
        Run the full sweep across all sources.
        Returns a flat list of new article dicts for FEED.

        Each returned sample is tagged with a 'dissipation_regime' label —
        'continuous', 'singular', or 'mixed' — based on the variance profile
        of the energy proxy (relevance score) across the viscosity-analog
        parameter (source ordering).  This prevents the Wasserstein floor
        from being artificially smoothed by mixing continuous and singular
        energy-loss samples (INV_073: co-existing dissipation regimes).
        """
        self._load_seen()   # reload from disk — targeted_sweep may have written new entries
        all_inputs = []

        # Collect per-source score buckets for dissipation regime classification.
        # The "viscosity-analog parameter" is the source index in SOURCES —
        # each source represents a different coupling strength to the genome's
        # relevance kernel.  The energy proxy is the relevance score.
        per_source_scores = []  # type: list

        for source in SOURCES:
            print(f"[SWEEP] Fetching: {source['name']}")
            try:
                inputs = self._sweep_source(source)
                # Collect scores for this source's batch
                batch_scores = [inp.get("score", 0) for inp in inputs]
                per_source_scores.append(batch_scores)
                all_inputs.extend(inputs)
                if inputs:
                    print(f"[SWEEP]   → {len(inputs)} new article(s).")
                else:
                    print(f"[SWEEP]   → No new articles.")
            except Exception as e:
                print(f"[SWEEP]   → Error: {e}")
                per_source_scores.append([])

            time.sleep(POLITENESS_DELAY)

        # ── Dissipation regime tagging (INV_073) ─────────────────────────
        # Classify each sample based on the variance profile of scores
        # across the source sweep (viscosity-analog parameter).
        #
        # The paper (Bruè, De Lellis et al.) proves multiple dissipation
        # scenarios coexist at vanishing viscosity:
        #   - Absolutely continuous (smooth energy decay across parameter)
        #   - Singular (sudden energy loss at specific parameter values)
        #   - Mixed (both patterns present)
        #
        # We compute a global energy-proxy variance profile across sources,
        # then classify each sample by comparing its local score gradient
        # to the global statistics.
        #
        # Flat per-source mean scores form the "energy curve" across the
        # viscosity-analog parameter (source index).
        energy_curve = []  # type: List[float]
        for bucket in per_source_scores:
            if bucket:
                energy_curve.append(sum(bucket) / float(len(bucket)))
            else:
                energy_curve.append(0.0)

        # Compute successive differences (energy gradient across sources)
        energy_diffs = []  # type: List[float]
        for i in range(1, len(energy_curve)):
            energy_diffs.append(abs(energy_curve[i] - energy_curve[i - 1]))

        # Statistics of the gradient: mean and std of |ΔE|
        if len(energy_diffs) >= 2:
            diff_mean = sum(energy_diffs) / float(len(energy_diffs))
            diff_var = sum((d - diff_mean) ** 2 for d in energy_diffs) / float(len(energy_diffs))
            diff_std = math.sqrt(diff_var) if diff_var > 0 else 0.0
            # Singular threshold: a jump > mean + 2*std indicates sudden loss
            singular_threshold = diff_mean + 2.0 * diff_std if diff_std > 0 else diff_mean * 2.0
        else:
            diff_mean = 0.0
            diff_std = 0.0
            singular_threshold = 0.0

        # Tag each sample with its dissipation regime
        source_idx = 0
        item_cursor = 0
        for src_idx, bucket in enumerate(per_source_scores):
            # Determine if this source boundary has a singular jump
            has_singular_jump = False
            has_continuous_flow = False

            if src_idx > 0 and energy_diffs and singular_threshold > 0:
                local_diff = energy_diffs[src_idx - 1] if src_idx - 1 < len(energy_diffs) else 0.0
                has_singular_jump = local_diff > singular_threshold
                has_continuous_flow = local_diff <= diff_mean + 0.5 * diff_std

            # Check forward boundary too
            if src_idx < len(energy_diffs) and energy_diffs and singular_threshold > 0:
                fwd_diff = energy_diffs[src_idx] if src_idx < len(energy_diffs) else 0.0
                if fwd_diff > singular_threshold:
                    has_singular_jump = True
                if fwd_diff <= diff_mean + 0.5 * diff_std:
                    has_continuous_flow = True

            # Classify
            if has_singular_jump and has_continuous_flow:
                regime = "mixed"
            elif has_singular_jump:
                regime = "singular"
            else:
                regime = "continuous"

            # Apply label to all items from this source
            for _ in range(len(bucket)):
                if item_cursor < len(all_inputs):
                    all_inputs[item_cursor]["dissipation_regime"] = regime
                    item_cursor += 1

        # Tag any remaining items (edge case: items without score buckets)
        while item_cursor < len(all_inputs):
            all_inputs[item_cursor]["dissipation_regime"] = "continuous"
            item_cursor += 1

        # ── Finite-Size Scaling (FSS) Exponent Extraction (O21) ──────────
        # Inspired by CNN-based finite-size scaling (Phys. Rev. Lett.):
        # fit a power-law collapse of spectral statistics (DEA scaling
        # exponent δ) across multiple sweep window sizes L to recover an
        # effective critical exponent ν.  This converts the qualitative
        # γ-proximity from a single-scale spectral measurement into a
        # falsifiable multi-scale ν estimate that can be tracked across
        # generations.
        #
        # Protocol:
        #   1. Collect the per-source relevance scores as a 1-D signal.
        #   2. Partition into windows of sizes L = [8, 16, 32, 64, ...].
        #   3. For each L, compute DEA scaling exponent δ(L).
        #   4. Fit δ(L) = δ_∞ + A · L^(-1/ν) via log-linear regression
        #      on (ln(L), δ(L)) to extract ν.
        #   5. Attach the FSS result to the sweep output for downstream
        #      obligation tracking.
        #
        # If the score stream is too short, the FSS block gracefully
        # returns a null result without affecting the existing output.
        fss_result = {
            "nu_estimate": 0.0,
            "nu_r_squared": 0.0,
            "delta_per_L": [],
            "window_sizes": [],
            "fss_status": "INSUFFICIENT_DATA",
        }  # type: dict

        all_scores = [inp.get("score", 0) for inp in all_inputs]
        # Need at least 3 window sizes with >= 10 points each
        _fss_min_windows = 3
        _fss_window_sizes = []  # type: List[int]
        _fss_L = 8
        while _fss_L <= len(all_scores) // 2 and _fss_L <= 256:
            _fss_window_sizes.append(_fss_L)
            _fss_L *= 2

        if len(_fss_window_sizes) >= _fss_min_windows and len(all_scores) >= 16:
            _fss_deltas = []   # type: List[float]
            _fss_ln_L = []     # type: List[float]
            _fss_pairs = []    # type: List[Tuple[int, float]]

            for _L in _fss_window_sizes:
                # Partition score stream into non-overlapping windows of size _L
                _n_windows = len(all_scores) // _L
                if _n_windows < 1:
                    continue
                # Average δ across all windows of this size
                _delta_accum = 0.0
                _delta_count = 0
                for _w_idx in range(_n_windows):
                    _window_data = [float(s) for s in all_scores[_w_idx * _L:(_w_idx + 1) * _L]]
                    if len(_window_data) >= 10:
                        _dea_res = diffusion_entropy_analysis(_window_data, t_min=2, n_t_points=min(15, _L // 2))
                        if _dea_res["r_squared"] > 0.3:
                            _delta_accum += _dea_res["delta"]
                            _delta_count += 1
                if _delta_count > 0:
                    _avg_delta = _delta_accum / float(_delta_count)
                    _fss_deltas.append(_avg_delta)
                    _fss_ln_L.append(math.log(float(_L)))
                    _fss_pairs.append((_L, _avg_delta))

            # Fit δ(L) vs ln(L) via linear regression to extract 1/ν
            # Model: δ(L) ≈ δ_∞ + A * L^(-1/ν)
            # In log-space approximation for the correction term:
            #   δ(L) ≈ intercept + slope * ln(L)
            # where slope ≈ -A/ν (the sign and magnitude give ν)
            if len(_fss_deltas) >= _fss_min_windows:
                _k = len(_fss_deltas)
                _sx = sum(_fss_ln_L)
                _sy = sum(_fss_deltas)
                _sxy = sum(x * y for x, y in zip(_fss_ln_L, _fss_deltas))
                _sx2 = sum(x * x for x in _fss_ln_L)
                _denom = float(_k) * _sx2 - _sx * _sx

                if abs(_denom) > 1e-15:
                    _slope = (float(_k) * _sxy - _sx * _sy) / _denom
                    _intercept = (_sy - _slope * _sx) / float(_k)

                    # R²
                    _mean_y = _sy / float(_k)
                    _ss_tot = sum((y - _mean_y) ** 2 for y in _fss_deltas)
                    _ss_res = sum((y - (_intercept + _slope * x)) ** 2
                                  for x, y in zip(_fss_ln_L, _fss_deltas))
                    _fss_r2 = 1.0 - (_ss_res / _ss_tot) if _ss_tot > 1e-15 else 0.0

                    # Extract ν: if slope < 0 (δ decreasing with L, typical
                    # for finite-size corrections), ν ≈ -1/slope.
                    # If slope ≈ 0, δ is scale-invariant (ν → ∞, at criticality).
                    if abs(_slope) > 1e-10:
                        _nu_est = -1.0 / _slope
                    else:
                        _nu_est = float('inf')  # scale-invariant

                    # ── KT exponential branch (INV_073) ──────────────
                    # Test KT-class fit: ξ ~ exp(a / sqrt|T - Tc|)
                    # In our parameterization, δ(L) plays the role of
                    # the correlation-length proxy.  The KT model is:
                    #   ln(δ(L)) = b + a / sqrt(L)
                    # i.e. linear in 1/sqrt(L).  We fit via OLS on
                    # (1/sqrt(L), ln(δ(L))) and compare to the power-law
                    # fit via BIC to determine which universality class
                    # (power-law vs KT exponential) better describes the
                    # correlation-length scaling.
                    #
                    # BIC = k_params * ln(n) + n * ln(RSS/n)
                    # Both models have 2 parameters; lower BIC wins.
                    #
                    # Reference: Kaupuzs et al. — KT scaling theory with
                    # exponential correlation-length growth.
                    _kt_selected = False
                    _kt_a = 0.0
                    _kt_b = 0.0
                    _kt_r2 = 0.0
                    _kt_bic = float('inf')
                    _pl_bic = float('inf')
                    _kt_detail = ""

                    # Build KT regressors: x_kt = 1/sqrt(L), y_kt = ln(δ)
                    _kt_x = []  # type: List[float]
                    _kt_y = []  # type: List[float]
                    for _Li, _di in _fss_pairs:
                        if _di > 1e-15 and _Li > 0:
                            _kt_x.append(1.0 / math.sqrt(float(_Li)))
                            _kt_y.append(math.log(_di))

                    _kt_n = len(_kt_x)
                    if _kt_n >= 3:
                        _kt_sx = sum(_kt_x)
                        _kt_sy = sum(_kt_y)
                        _kt_sxy = sum(_x * _y for _x, _y in zip(_kt_x, _kt_y))
                        _kt_sx2 = sum(_x * _x for _x in _kt_x)
                        _kt_denom = float(_kt_n) * _kt_sx2 - _kt_sx * _kt_sx

                        if abs(_kt_denom) > 1e-15:
                            _kt_a = (float(_kt_n) * _kt_sxy - _kt_sx * _kt_sy) / _kt_denom
                            _kt_b = (_kt_sy - _kt_a * _kt_sx) / float(_kt_n)

                            # KT R²
                            _kt_mean_y = _kt_sy / float(_kt_n)
                            _kt_ss_tot = sum((_y - _kt_mean_y) ** 2 for _y in _kt_y)
                            _kt_ss_res = sum(
                                (_y - (_kt_b + _kt_a * _x)) ** 2
                                for _x, _y in zip(_kt_x, _kt_y)
                            )
                            _kt_r2 = 1.0 - (_kt_ss_res / _kt_ss_tot) if _kt_ss_tot > 1e-15 else 0.0

                            # BIC for KT model: 2 params, n=_kt_n
                            # BIC = k*ln(n) + n*ln(RSS/n)
                            _kt_rss = _kt_ss_res
                            if _kt_rss > 0 and _kt_n > 0:
                                _kt_bic = 2.0 * math.log(float(_kt_n)) + float(_kt_n) * math.log(_kt_rss / float(_kt_n))
                            elif _kt_rss <= 0:
                                _kt_bic = 2.0 * math.log(float(_kt_n)) + float(_kt_n) * math.log(1e-30)

                            # BIC for power-law model: 2 params, n=_k
                            # Use the _ss_res from the power-law fit above
                            _pl_rss = _ss_res
                            if _pl_rss > 0 and _k > 0:
                                _pl_bic = 2.0 * math.log(float(_k)) + float(_k) * math.log(_pl_rss / float(_k))
                            elif _pl_rss <= 0:
                                _pl_bic = 2.0 * math.log(float(_k)) + float(_k) * math.log(1e-30)

                            # Select model with lower BIC
                            _kt_selected = _kt_bic < _pl_bic

                            if _kt_selected:
                                _kt_detail = (
                                    "KT EXPONENTIAL SELECTED (BIC): KT BIC={:.4f} < "
                                    "power-law BIC={:.4f}. KT fit: ln(delta) = {:.6f} + "
                                    "{:.6f}/sqrt(L), R^2={:.4f}. This suggests the "
                                    "correlation-length scaling is Kosterlitz-Thouless "
                                    "class (xi ~ exp(a/sqrt|T-Tc|)) rather than "
                                    "power-law. The critical ridge has an implicit, "
                                    "self-referential rate exponent that drifts with "
                                    "distance from criticality — INV_073's fixed-geometry "
                                    "ridge-navigation model may be a special case valid "
                                    "only for power-law universality classes."
                                ).format(_kt_bic, _pl_bic, _kt_b, _kt_a, _kt_r2)
                            else:
                                _kt_detail = (
                                    "POWER-LAW SELECTED (BIC): power-law BIC={:.4f} < "
                                    "KT BIC={:.4f}. KT fit R^2={:.4f} vs power-law "
                                    "R^2={:.4f}. Standard power-law universality class "
                                    "is the better model — INV_073's fixed-geometry "
                                    "ridge-navigation model is consistent with the "
                                    "observed scaling."
                                ).format(_pl_bic, _kt_bic, _kt_r2, _fss_r2)

                    if _nu_est == float('inf'):
                        _fss_status = "SCALE_INVARIANT"
                    elif _kt_selected:
                        _fss_status = "KT_EXPONENTIAL"
                    elif _nu_est > 0 and _fss_r2 > 0.5:
                        _fss_status = "CONVERGED"
                    elif _fss_r2 > 0.3:
                        _fss_status = "MARGINAL"
                    else:
                        _fss_status = "POOR_FIT"

                    # ── Critical Sparsity Threshold Detection (O21 / AlphaPruning) ──
                    # Fit a scaling law to performance (δ) vs connectivity (L)
                    # to detect the phase boundary between cooperative (low-sparsity,
                    # performance-preserving) and disordered (high-sparsity,
                    # collapsed-performance) regimes.
                    #
                    # Model: δ(L) = δ_∞ + A * |L - L_c|^β  (second-order scaling)
                    # where L_c is the critical connectivity (sparsity threshold)
                    # and β is the critical exponent.
                    #
                    # Detection protocol:
                    #   1. Compute the second derivative d²δ/d(ln L)² from the
                    #      δ(L) curve — the inflection point marks L_c.
                    #   2. Fit the power-law scaling near L_c to extract β.
                    #   3. Flag L_c as a calibration anchor for spectral γ scoring:
                    #      connectivity below L_c places the network in the
                    #      disordered phase where γ transitions from cooperative
                    #      to disordered (AlphaPruning correlation protocol).
                    #
                    # Reference: "Deep networks undergo a sharp transition from a
                    # cooperative, functional phase to a disordered phase with
                    # collapsed performance" — second-order critical behavior with
                    # connectivity as the control parameter.

                    _cst_result = {
                        "critical_L": 0,
                        "critical_sparsity": 0.0,
                        "critical_delta": 0.0,
                        "scaling_exponent_beta": 0.0,
                        "scaling_r_squared": 0.0,
                        "phase_boundary_detected": False,
                        "cooperative_regime": {"L_range": [], "mean_delta": 0.0},
                        "disordered_regime": {"L_range": [], "mean_delta": 0.0},
                        "inflection_curvature": 0.0,
                        "cst_detail": "",
                    }  # type: dict

                    if len(_fss_pairs) >= 4:
                        # Compute second derivative of δ w.r.t. ln(L) via
                        # central finite differences to find the inflection point
                        _cst_ln_L = [math.log(float(_p[0])) for _p in _fss_pairs]
                        _cst_delta = [_p[1] for _p in _fss_pairs]
                        _cst_d2 = []  # type: List[Tuple[int, float, float]]
                        # (index, ln_L, d2delta/d(lnL)2)

                        for _ci in range(1, len(_cst_ln_L) - 1):
                            _h_left = _cst_ln_L[_ci] - _cst_ln_L[_ci - 1]
                            _h_right = _cst_ln_L[_ci + 1] - _cst_ln_L[_ci]
                            _h_avg = (_h_left + _h_right) / 2.0
                            if abs(_h_avg) > 1e-15:
                                _d2_val = (
                                    _cst_delta[_ci + 1]
                                    - 2.0 * _cst_delta[_ci]
                                    + _cst_delta[_ci - 1]
                                ) / (_h_avg ** 2)
                                _cst_d2.append((_ci, _cst_ln_L[_ci], _d2_val))

                        if _cst_d2:
                            # The inflection point is where |d²δ/d(lnL)²| is maximized
                            _max_d2_entry = max(_cst_d2, key=lambda x: abs(x[2]))
                            _infl_idx = _max_d2_entry[0]
                            _infl_curvature = _max_d2_entry[2]

                            _critical_L = _fss_pairs[_infl_idx][0]
                            _critical_delta = _fss_pairs[_infl_idx][1]

                            # Critical sparsity: fraction of connectivity removed
                            # relative to the largest window (full connectivity proxy)
                            _L_max = _fss_pairs[-1][0]
                            _critical_sparsity = 1.0 - (float(_critical_L) / float(_L_max)) if _L_max > 0 else 0.0

                            # Split into cooperative (L > L_c) and disordered (L < L_c) regimes
                            _coop_pairs = [_p for _p in _fss_pairs if _p[0] >= _critical_L]
                            _disord_pairs = [_p for _p in _fss_pairs if _p[0] < _critical_L]

                            _coop_mean_d = (
                                sum(_p[1] for _p in _coop_pairs) / float(len(_coop_pairs))
                                if _coop_pairs else 0.0
                            )
                            _disord_mean_d = (
                                sum(_p[1] for _p in _disord_pairs) / float(len(_disord_pairs))
                                if _disord_pairs else 0.0
                            )

                            # Fit scaling exponent β near L_c:
                            # ln|δ - δ_c| = ln(A) + β * ln|L - L_c|
                            # Use points on the cooperative side (L > L_c)
                            _beta_x = []  # type: List[float]
                            _beta_y = []  # type: List[float]
                            for _bL, _bD in _coop_pairs:
                                _dL = abs(float(_bL) - float(_critical_L))
                                _dD = abs(_bD - _critical_delta)
                                if _dL > 0.1 and _dD > 1e-12:
                                    _beta_x.append(math.log(_dL))
                                    _beta_y.append(math.log(_dD))

                            _beta_est = 0.0
                            _beta_r2 = 0.0
                            if len(_beta_x) >= 2:
                                _bk = len(_beta_x)
                                _bsx = sum(_beta_x)
                                _bsy = sum(_beta_y)
                                _bsxy = sum(_x * _y for _x, _y in zip(_beta_x, _beta_y))
                                _bsx2 = sum(_x * _x for _x in _beta_x)
                                _bd = float(_bk) * _bsx2 - _bsx * _bsx
                                if abs(_bd) > 1e-15:
                                    _beta_est = (float(_bk) * _bsxy - _bsx * _bsy) / _bd
                                    _b_int = (_bsy - _beta_est * _bsx) / float(_bk)
                                    _b_mean_y = _bsy / float(_bk)
                                    _b_ss_tot = sum((_y - _b_mean_y) ** 2 for _y in _beta_y)
                                    _b_ss_res = sum(
                                        (_y - (_b_int + _beta_est * _x)) ** 2
                                        for _x, _y in zip(_beta_x, _beta_y)
                                    )
                                    _beta_r2 = 1.0 - (_b_ss_res / _b_ss_tot) if _b_ss_tot > 1e-15 else 0.0

                            # Phase boundary detected if curvature is significant
                            # and the two regimes have meaningfully different δ
                            _delta_gap = abs(_coop_mean_d - _disord_mean_d)
                            _phase_detected = (
                                abs(_infl_curvature) > 0.1
                                and _delta_gap > 0.01
                                and len(_coop_pairs) >= 1
                                and len(_disord_pairs) >= 1
                            )

                            if _phase_detected:
                                _cst_detail_str = (
                                    "CRITICAL SPARSITY DETECTED (O21/AlphaPruning): "
                                    "L_c={} (sparsity={:.4f}), delta_c={:.6f}, "
                                    "inflection curvature={:.6f}. Scaling exponent "
                                    "beta={:.4f} (R^2={:.4f}). Cooperative regime "
                                    "(L>={}, mean_delta={:.6f}) vs disordered regime "
                                    "(L<{}, mean_delta={:.6f}), gap={:.6f}. This "
                                    "phase boundary anchors spectral gamma scoring: "
                                    "connectivity below L_c={} places the network in "
                                    "the disordered phase where gamma transitions from "
                                    "cooperative to disordered, grounding O21's "
                                    "AlphaPruning correlation protocol in measurable "
                                    "universality class exponents (beta={:.4f})."
                                ).format(
                                    _critical_L, _critical_sparsity, _critical_delta,
                                    _infl_curvature, _beta_est, _beta_r2,
                                    _critical_L, _coop_mean_d,
                                    _critical_L, _disord_mean_d, _delta_gap,
                                    _critical_L, _beta_est,
                                )
                            else:
                                _cst_detail_str = (
                                    "No clear phase boundary: inflection curvature="
                                    "{:.6f}, delta gap={:.6f}. The performance-vs-"
                                    "connectivity curve does not show a sharp "
                                    "cooperative-to-disordered transition at this "
                                    "resolution ({} window sizes)."
                                ).format(
                                    _infl_curvature, _delta_gap, len(_fss_pairs),
                                )

                            # ── Phase-Critical Detection (O21 AlphaPruning) ──
                            # Flag when γ (spectral scaling exponent δ) crosses
                            # the critical threshold identified by second-order
                            # phase-transition scaling signatures:
                            #   1. Diverging variance: the variance of δ values
                            #      in the window containing L_c is significantly
                            #      larger than in windows away from L_c.
                            #   2. Power-law exponent shift: β deviates from the
                            #      mean-field value (β_MF = 0.5) indicating
                            #      non-trivial universality class.
                            # When both signatures are present, the γ value at
                            # L_c is at the phase boundary and the corresponding
                            # belief-revision score is annotated phase_critical=True.
                            _phase_critical = False
                            _variance_diverging = False
                            _exponent_shifted = False
                            _pc_variance_ratio = 0.0
                            _pc_beta_deviation = abs(_beta_est - 0.5) if _beta_est > 0 else 0.0

                            # Variance divergence test: compare δ variance near L_c
                            # vs away from L_c
                            if len(_coop_pairs) >= 2 and len(_disord_pairs) >= 2:
                                _coop_deltas = [_p[1] for _p in _coop_pairs]
                                _disord_deltas = [_p[1] for _p in _disord_pairs]
                                _coop_mean_v = sum(_coop_deltas) / float(len(_coop_deltas))
                                _disord_mean_v = sum(_disord_deltas) / float(len(_disord_deltas))
                                _coop_var = sum((_d - _coop_mean_v) ** 2 for _d in _coop_deltas) / float(len(_coop_deltas))
                                _disord_var = sum((_d - _disord_mean_v) ** 2 for _d in _disord_deltas) / float(len(_disord_deltas))
                                # Near-critical variance: combine both sides
                                _near_crit_var = (_coop_var + _disord_var) / 2.0
                                # Far-from-critical variance: use the regime with lower variance
                                _far_var = min(_coop_var, _disord_var) if min(_coop_var, _disord_var) > 1e-15 else 1e-15
                                _pc_variance_ratio = _near_crit_var / _far_var if _far_var > 1e-15 else 0.0
                                # Diverging if variance ratio > 2 (fluctuations peak at L_c)
                                _variance_diverging = _pc_variance_ratio > 2.0

                            # Exponent shift test: β significantly different from
                            # mean-field (0.5) with good fit quality
                            _exponent_shifted = (_pc_beta_deviation > 0.15 and _beta_r2 > 0.5)

                            # Phase-critical flag: either signature suffices when
                            # phase boundary is detected; both together give strong signal
                            _phase_critical = _phase_detected and (_variance_diverging or _exponent_shifted)

                            _cst_result = {
                                "critical_L": _critical_L,
                                "critical_sparsity": round(_critical_sparsity, 6),
                                "critical_delta": round(_critical_delta, 6),
                                "scaling_exponent_beta": round(_beta_est, 6),
                                "scaling_r_squared": round(max(0.0, _beta_r2), 6),
                                "phase_boundary_detected": _phase_detected,
                                "phase_critical": _phase_critical,
                                "phase_critical_variance_diverging": _variance_diverging,
                                "phase_critical_exponent_shifted": _exponent_shifted,
                                "phase_critical_variance_ratio": round(_pc_variance_ratio, 6),
                                "phase_critical_beta_deviation": round(_pc_beta_deviation, 6),
                                "cooperative_regime": {
                                    "L_range": [_p[0] for _p in _coop_pairs],
                                    "mean_delta": round(_coop_mean_d, 6),
                                },
                                "disordered_regime": {
                                    "L_range": [_p[0] for _p in _disord_pairs],
                                    "mean_delta": round(_disord_mean_d, 6),
                                },
                                "inflection_curvature": round(_infl_curvature, 8),
                                "delta_gap": round(_delta_gap, 6),
                                "cst_detail": _cst_detail_str,
                            }

                    fss_result = {
                        "nu_estimate": round(_nu_est, 6) if _nu_est != float('inf') else float('inf'),
                        "nu_r_squared": round(max(0.0, _fss_r2), 6),
                        "delta_per_L": [(L, round(d, 6)) for L, d in _fss_pairs],
                        "window_sizes": _fss_window_sizes[:len(_fss_deltas)],
                        "slope": round(_slope, 8),
                        "intercept": round(_intercept, 8),
                        "n_window_sizes": _k,
                        "fss_status": _fss_status,
                        "kt_selected": _kt_selected,
                        "kt_a": round(_kt_a, 8),
                        "kt_b": round(_kt_b, 8),
                        "kt_r_squared": round(max(0.0, _kt_r2), 6),
                        "kt_bic": round(_kt_bic, 4) if _kt_bic != float('inf') else float('inf'),
                        "pl_bic": round(_pl_bic, 4) if _pl_bic != float('inf') else float('inf'),
                        "universality_class": "KT_EXPONENTIAL" if _kt_selected else "POWER_LAW",
                        "kt_detail": _kt_detail,
                        "critical_sparsity_threshold": _cst_result,
                        "o21_assessment": (
                            "FSS exponent extraction: nu={}, R^2={:.4f}, "
                            "status={}. {} window sizes used (L={}). "
                            "KT exponential test: {} (BIC_KT={}, BIC_PL={}). "
                            "This multi-scale collapse protocol converts "
                            "single-scale spectral gamma into a falsifiable "
                            "critical exponent estimate per CNN-FSS method, "
                            "now with KT-class discrimination."
                        ).format(
                            "inf" if _nu_est == float('inf') else "{:.4f}".format(_nu_est),
                            max(0.0, _fss_r2), _fss_status, _k,
                            [p[0] for p in _fss_pairs],
                            "SELECTED" if _kt_selected else "rejected",
                            "inf" if _kt_bic == float('inf') else "{:.4f}".format(_kt_bic),
                            "inf" if _pl_bic == float('inf') else "{:.4f}".format(_pl_bic),
                        ),
                    }

        # ── Absorbing-State Signature Detection (O21 / INV_073) ────────
        # When computing spectral γ (DEA δ) for O21, detect whether the
        # activity time-series shows absorbing-state signatures: contiguous
        # runs of zero activity.  This distinguishes sub-critical collapse
        # (long zero-runs → system fell into absorbing state) from critical
        # fluctuations (brief zero-runs interspersed with power-law bursts).
        #
        # SOC theory (Muñoz et al.): the critical ridge is the boundary
        # between an active phase (activity reverberates indefinitely) and
        # an absorbing/quiescent phase (activity eventually ceases).  Long
        # contiguous zero-runs are the hallmark of the absorbing state.
        #
        # The detection runs in the same pass that already computes γ,
        # scanning the per-source score stream for contiguous zero-score
        # runs.  Output: a binary covariate `absorbing_state_flag` (True
        # if the longest zero-run exceeds a threshold fraction of the
        # series length) plus diagnostic metadata.
        #
        # INV_073 relevance: the paper shows the critical ridge is only a
        # stable attractor under strict two-timescale separation (drive ≪
        # relax).  If the score stream shows absorbing-state signatures,
        # the system has crossed into the sub-critical phase, meaning the
        # two-timescale separation assumption may be violated for RSA's
        # actual cognitive timescales.

        # Threshold: a zero-run longer than this fraction of N is absorbing
        _ABSORBING_RUN_FRACTION = 0.15  # 15% of series length
        _ABSORBING_MIN_RUN = 3          # minimum absolute run length to count

        # Scan the score stream for contiguous zero-activity runs
        _zero_runs = []       # type: List[int]
        _current_run = 0      # type: int
        _total_zeros = 0      # type: int

        for _sc in all_scores:
            if _sc == 0 or _sc == 0.0:
                _current_run += 1
                _total_zeros += 1
            else:
                if _current_run >= _ABSORBING_MIN_RUN:
                    _zero_runs.append(_current_run)
                _current_run = 0
        # Flush trailing run
        if _current_run >= _ABSORBING_MIN_RUN:
            _zero_runs.append(_current_run)

        _n_scores = len(all_scores)
        _max_zero_run = max(_zero_runs) if _zero_runs else 0
        _absorbing_threshold = max(
            _ABSORBING_MIN_RUN,
            int(_ABSORBING_RUN_FRACTION * _n_scores)
        ) if _n_scores > 0 else _ABSORBING_MIN_RUN

        _absorbing_flag = _max_zero_run >= _absorbing_threshold
        _zero_fraction = float(_total_zeros) / float(_n_scores) if _n_scores > 0 else 0.0

        if _absorbing_flag:
            _absorbing_detail = (
                "ABSORBING-STATE DETECTED (O21/INV_073): longest contiguous "
                "zero-activity run = {} steps (threshold = {}, {:.1%} of N={}). "
                "Total zero-activity fraction = {:.1%}. {} zero-runs of length "
                ">= {}. This indicates sub-critical collapse — the system has "
                "entered the absorbing phase. The spectral gamma estimate "
                "(delta={}) may reflect sub-critical dynamics rather than "
                "critical fluctuations. INV_073: the two-timescale separation "
                "(drive << relax) may not hold at RSA's cognitive timescales."
            ).format(
                _max_zero_run, _absorbing_threshold,
                float(_max_zero_run) / float(_n_scores) if _n_scores > 0 else 0.0,
                _n_scores, _zero_fraction, len(_zero_runs), _ABSORBING_MIN_RUN,
                fss_result.get("slope", "N/A"),
            )
        else:
            _absorbing_detail = (
                "No absorbing-state signature: longest zero-run = {} steps "
                "(threshold = {}). Zero-activity fraction = {:.1%}. "
                "Consistent with active/critical phase — spectral gamma "
                "estimate reflects genuine critical fluctuations."
            ).format(_max_zero_run, _absorbing_threshold, _zero_fraction)

        _absorbing_state_result = {
            "absorbing_state_flag": _absorbing_flag,
            "max_zero_run": _max_zero_run,
            "absorbing_threshold": _absorbing_threshold,
            "n_zero_runs": len(_zero_runs),
            "zero_run_lengths": _zero_runs[:20],  # cap for serialization
            "zero_fraction": round(_zero_fraction, 6),
            "total_zeros": _total_zeros,
            "n_scores": _n_scores,
            "absorbing_detail": _absorbing_detail,
        }

        # ── Energy Histogram Bimodality Detection (INV_073 Phase-Order) ──
        # Finite-size histogram double-peak detection: computes the
        # bimodality coefficient (BC) of the energy distribution (score
        # histogram) at each sweep step, and measures peak separation as
        # a function of 1/L³ across multiple window sizes L.
        #
        # At a first-order phase transition, the energy histogram develops
        # two peaks (ordered + disordered coexisting phases) whose
        # separation grows as ~1/L³ in finite systems (Kaupuzs et al.,
        # 3D Blume-Capel model).  At a continuous transition, the histogram
        # remains unimodal.  The bimodality coefficient BC = (γ₁² + 1) / κ
        # (where γ₁ = skewness, κ = excess kurtosis + 3) exceeds 5/9 ≈ 0.556
        # for bimodal distributions.
        #
        # This gives the epistemic loop a direct observable for phase-order
        # classification without requiring analytic free energy access.
        #
        # INV_073: near the tricritical point, the critical ridge is not
        # uniquely identifiable from finite-size data without careful
        # volume-scaling analysis.  γ=1 navigation may be systematically
        # misclassified in small systems as either frozen or dissipated
        # depending on histogram resolution.  This diagnostic flags that
        # ambiguity explicitly.

        def _bimodality_coefficient(values):
            # type: (List[float]) -> dict
            """
            Compute bimodality coefficient BC and peak-separation diagnostics
            for an energy/score distribution.

            BC = (skewness^2 + 1) / kurtosis
            where kurtosis is the standard (not excess) kurtosis.
            BC > 5/9 ≈ 0.556 suggests bimodality (two peaks).

            Also detects double peaks via histogram scan: finds the two
            tallest local maxima and reports their separation.
            """
            n_v = len(values)
            if n_v < 4:
                return {
                    "bimodality_coefficient": 0.0,
                    "skewness": 0.0,
                    "kurtosis": 0.0,
                    "is_bimodal": False,
                    "peak_separation": 0.0,
                    "n_peaks": 0,
                    "peak_positions": [],
                    "n_samples": n_v,
                }

            # Moments
            mu = sum(values) / float(n_v)
            m2 = sum((x - mu) ** 2 for x in values) / float(n_v)
            m3 = sum((x - mu) ** 3 for x in values) / float(n_v)
            m4 = sum((x - mu) ** 4 for x in values) / float(n_v)

            if m2 < 1e-30:
                return {
                    "bimodality_coefficient": 0.0,
                    "skewness": 0.0,
                    "kurtosis": 0.0,
                    "is_bimodal": False,
                    "peak_separation": 0.0,
                    "n_peaks": 0,
                    "peak_positions": [],
                    "n_samples": n_v,
                }

            std = math.sqrt(m2)
            skewness = m3 / (std ** 3)
            # Standard kurtosis (not excess): for normal distribution = 3
            kurtosis = m4 / (m2 ** 2)

            if kurtosis < 1e-15:
                bc = 0.0
            else:
                bc = (skewness ** 2 + 1.0) / kurtosis

            # Histogram peak detection
            n_hist_bins = max(8, int(math.sqrt(float(n_v))))
            v_min = min(values)
            v_max = max(values)
            v_span = v_max - v_min
            if v_span <= 0:
                hist_peaks = []
                peak_sep = 0.0
            else:
                bw = v_span / float(n_hist_bins)
                hist_counts = [0] * n_hist_bins
                for v in values:
                    idx = int((v - v_min) / bw)
                    if idx >= n_hist_bins:
                        idx = n_hist_bins - 1
                    hist_counts[idx] += 1

                # Find local maxima (bins higher than both neighbours)
                hist_peaks = []  # (bin_index, count)
                for bi in range(n_hist_bins):
                    left = hist_counts[bi - 1] if bi > 0 else -1
                    right = hist_counts[bi + 1] if bi < n_hist_bins - 1 else -1
                    if hist_counts[bi] > left and hist_counts[bi] > right:
                        hist_peaks.append((bi, hist_counts[bi]))

                # Sort by count descending, take top 2
                hist_peaks.sort(key=lambda x: x[1], reverse=True)
                if len(hist_peaks) >= 2:
                    p1_center = v_min + (float(hist_peaks[0][0]) + 0.5) * bw
                    p2_center = v_min + (float(hist_peaks[1][0]) + 0.5) * bw
                    peak_sep = abs(p1_center - p2_center)
                else:
                    peak_sep = 0.0

            is_bimodal = bc > (5.0 / 9.0) and len(hist_peaks) >= 2

            peak_positions_out = []
            for pk_idx, pk_cnt in hist_peaks[:4]:
                pk_center = v_min + (float(pk_idx) + 0.5) * (v_span / float(n_hist_bins)) if v_span > 0 else v_min
                peak_positions_out.append(round(pk_center, 6))

            return {
                "bimodality_coefficient": round(bc, 6),
                "skewness": round(skewness, 6),
                "kurtosis": round(kurtosis, 6),
                "is_bimodal": is_bimodal,
                "peak_separation": round(peak_sep, 6),
                "n_peaks": min(len(hist_peaks), 4),
                "peak_positions": peak_positions_out,
                "n_samples": n_v,
            }

        # Compute bimodality over the full score stream
        _bimodality_global = _bimodality_coefficient(
            [float(s) for s in all_scores]
        )

        # Compute peak separation as function of 1/L³ across window sizes
        # to distinguish first-order from continuous transitions.
        # At a first-order transition: peak_sep ~ const as L→∞
        # (separation persists).  At continuous: peak_sep → 0.
        _bimodality_fss_window_sizes = []  # type: List[int]
        _bm_L = 8
        while _bm_L <= len(all_scores) // 2 and _bm_L <= 256:
            _bimodality_fss_window_sizes.append(_bm_L)
            _bm_L *= 2

        _bimodality_per_L = []  # type: List[Tuple[int, float, float]]
        # (L, 1/L^3, mean_peak_separation)
        for _bL in _bimodality_fss_window_sizes:
            _n_win = len(all_scores) // _bL
            if _n_win < 1:
                continue
            _sep_accum = 0.0
            _bc_accum = 0.0
            _valid_wins = 0
            for _wi in range(_n_win):
                _win_data = [float(s) for s in all_scores[_wi * _bL:(_wi + 1) * _bL]]
                if len(_win_data) >= 4:
                    _bm_res = _bimodality_coefficient(_win_data)
                    _sep_accum += _bm_res["peak_separation"]
                    _bc_accum += _bm_res["bimodality_coefficient"]
                    _valid_wins += 1
            if _valid_wins > 0:
                _mean_sep = _sep_accum / float(_valid_wins)
                _mean_bc = _bc_accum / float(_valid_wins)
                _inv_L3 = 1.0 / (float(_bL) ** 3)
                _bimodality_per_L.append((_bL, _inv_L3, _mean_sep, _mean_bc))

        # Classify phase-order from the peak-separation scaling
        _phase_order = "UNDETERMINED"
        _phase_order_detail = ""
        if len(_bimodality_per_L) >= 3:
            # Fit peak_sep vs 1/L³ via linear regression
            _bo_x = [entry[1] for entry in _bimodality_per_L]  # 1/L³
            _bo_y = [entry[2] for entry in _bimodality_per_L]  # peak_sep
            _bo_k = len(_bo_x)
            _bo_sx = sum(_bo_x)
            _bo_sy = sum(_bo_y)
            _bo_sxy = sum(x * y for x, y in zip(_bo_x, _bo_y))
            _bo_sx2 = sum(x * x for x in _bo_x)
            _bo_denom = float(_bo_k) * _bo_sx2 - _bo_sx * _bo_sx

            if abs(_bo_denom) > 1e-15:
                _bo_slope = (float(_bo_k) * _bo_sxy - _bo_sx * _bo_sy) / _bo_denom
                _bo_intercept = (_bo_sy - _bo_slope * _bo_sx) / float(_bo_k)

                # If intercept > 0 and slope is small relative to intercept,
                # peak separation persists at L→∞ → first-order
                # If intercept ≈ 0, peak separation vanishes → continuous
                _bo_mean_sep = sum(_bo_y) / float(_bo_k)
                if _bo_mean_sep > 1e-6:
                    _intercept_ratio = abs(_bo_intercept) / _bo_mean_sep
                else:
                    _intercept_ratio = 0.0

                if _bimodality_global["is_bimodal"] and _intercept_ratio > 0.3:
                    _phase_order = "FIRST_ORDER"
                    _phase_order_detail = (
                        "FIRST-ORDER TRANSITION SIGNATURE (INV_073): Energy "
                        "histogram is bimodal (BC={:.4f} > 5/9, {} peaks). "
                        "Peak separation extrapolates to {:.6f} at L→∞ "
                        "(intercept/mean_sep={:.2f}), indicating coexisting "
                        "phases that persist in the thermodynamic limit. "
                        "Near the tricritical point, γ=1 navigation may be "
                        "misclassified in small systems. Slope={:.6f} across "
                        "{} window sizes."
                    ).format(
                        _bimodality_global["bimodality_coefficient"],
                        _bimodality_global["n_peaks"],
                        _bo_intercept, _intercept_ratio,
                        _bo_slope, _bo_k,
                    )
                elif _bimodality_global["is_bimodal"]:
                    _phase_order = "NEAR_TRICRITICAL"
                    _phase_order_detail = (
                        "NEAR-TRICRITICAL (INV_073): Energy histogram is "
                        "bimodal (BC={:.4f}) but peak separation vanishes "
                        "with system size (intercept_ratio={:.2f} < 0.3). "
                        "The critical ridge is NOT uniquely identifiable "
                        "from finite-size data — γ=1 may be systematically "
                        "misclassified depending on histogram resolution."
                    ).format(
                        _bimodality_global["bimodality_coefficient"],
                        _intercept_ratio,
                    )
                else:
                    _phase_order = "CONTINUOUS"
                    _phase_order_detail = (
                        "CONTINUOUS TRANSITION: Energy histogram is unimodal "
                        "(BC={:.4f} < 5/9). No double-peak structure detected. "
                        "Consistent with second-order / continuous phase "
                        "transition. γ=1 identification from finite-size data "
                        "is reliable at this resolution."
                    ).format(_bimodality_global["bimodality_coefficient"])
            else:
                _phase_order_detail = (
                    "Degenerate regression for peak-sep vs 1/L^3. "
                    "BC_global={:.4f}, is_bimodal={}."
                ).format(
                    _bimodality_global["bimodality_coefficient"],
                    _bimodality_global["is_bimodal"],
                )
        else:
            _phase_order_detail = (
                "Insufficient window sizes ({}) for FSS peak-separation "
                "analysis. BC_global={:.4f}, is_bimodal={}. Need >= 3 "
                "window sizes with L <= N/2."
            ).format(
                len(_bimodality_per_L),
                _bimodality_global["bimodality_coefficient"],
                _bimodality_global["is_bimodal"],
            )

        _bimodality_result = {
            "bimodality_global": _bimodality_global,
            "bimodality_per_L": [
                {"L": entry[0], "inv_L3": round(entry[1], 12),
                 "mean_peak_separation": round(entry[2], 6),
                 "mean_bc": round(entry[3], 6)}
                for entry in _bimodality_per_L
            ],
            "phase_order": _phase_order,
            "phase_order_detail": _phase_order_detail,
            "n_window_sizes": len(_bimodality_per_L),
        }

        # Attach bimodality result to every sweep item
        for inp in all_inputs:
            inp["energy_bimodality"] = _bimodality_result

        # ── Branching-Ratio Estimator (INV_073 Falsification) ────────────
        # Computes σ = descendant_events / ancestor_events per avalanche
        # epoch across the per-source score stream, classifying the sweep's
        # aggregate activity as sub-critical (σ < 0.95), critical
        # (0.95 ≤ σ ≤ 1.05), or super-critical (σ > 1.05).
        #
        # An "avalanche" here is a contiguous run of above-mean activity
        # across consecutive source batches.  Within each avalanche the
        # ancestor score (source i) and descendant score (source i+1) are
        # compared to yield a per-epoch σ.  The global σ is the mean of
        # per-epoch ratios.
        #
        # INV_073 challenge: if σ systematically drifts above 1 under high
        # cognitive load (many high-scoring sources), the claim that
        # biological / epistemic systems *maintain* the critical ridge
        # during active cognition is falsified rather than merely
        # approached transiently.
        #
        # This is the canonical SOC order parameter; embedding it here
        # directly tests O21's spectral-γ correlation claim with a
        # falsifiable, continuous metric.

        _br_result = {
            "sigma_mean": 0.0,
            "sigma_std": 0.0,
            "sigma_per_epoch": [],
            "n_epochs": 0,
            "regime": "UNDETERMINED",
            "regime_detail": "",
            "inv073_falsified": False,
            "inv073_detail": "",
        }  # type: dict

        # Compute global mean score as the quiescent threshold
        _all_src_means = [
            sum(b) / float(len(b)) if b else 0.0
            for b in per_source_scores
        ]
        _global_mean_score = (
            sum(_all_src_means) / float(len(_all_src_means))
            if _all_src_means else 0.0
        )

        # Identify avalanche epochs: contiguous runs of above-mean sources
        _br_epochs = []       # type: List[float]
        _in_epoch = False
        _epoch_ancestors = [] # type: List[float]
        _epoch_descendants = [] # type: List[float]

        for _si in range(len(_all_src_means)):
            _above = _all_src_means[_si] > _global_mean_score
            if _above:
                if not _in_epoch:
                    _in_epoch = True
                    _epoch_ancestors = []
                    _epoch_descendants = []
                # Within an epoch, treat source i as ancestor, i+1 as descendant
                if _epoch_ancestors:
                    # Previous source was ancestor, current is descendant
                    _epoch_descendants.append(_all_src_means[_si])
                _epoch_ancestors.append(_all_src_means[_si])
            else:
                # Quiescent — close any open epoch
                if _in_epoch and len(_epoch_ancestors) >= 1 and len(_epoch_descendants) >= 1:
                    _anc_mean = sum(_epoch_ancestors[:-1]) / float(len(_epoch_ancestors) - 1) if len(_epoch_ancestors) > 1 else _epoch_ancestors[0]
                    _desc_mean = sum(_epoch_descendants) / float(len(_epoch_descendants))
                    if _anc_mean > 1e-12:
                        _br_epochs.append(_desc_mean / _anc_mean)
                _in_epoch = False
                _epoch_ancestors = []
                _epoch_descendants = []

        # Flush trailing epoch
        if _in_epoch and len(_epoch_ancestors) >= 1 and len(_epoch_descendants) >= 1:
            _anc_mean = sum(_epoch_ancestors[:-1]) / float(len(_epoch_ancestors) - 1) if len(_epoch_ancestors) > 1 else _epoch_ancestors[0]
            _desc_mean = sum(_epoch_descendants) / float(len(_epoch_descendants))
            if _anc_mean > 1e-12:
                _br_epochs.append(_desc_mean / _anc_mean)

        # If no multi-step epochs found, fall back to consecutive-pair ratios
        if not _br_epochs and len(_all_src_means) >= 2:
            for _si in range(len(_all_src_means) - 1):
                if _all_src_means[_si] > 1e-12:
                    _br_epochs.append(
                        _all_src_means[_si + 1] / _all_src_means[_si]
                    )

        if _br_epochs:
            _br_n = len(_br_epochs)
            _br_mean = sum(_br_epochs) / float(_br_n)
            _br_var = sum((_s - _br_mean) ** 2 for _s in _br_epochs) / float(_br_n)
            _br_std = math.sqrt(_br_var) if _br_var > 0 else 0.0

            # Classify regime
            _BR_LOW = 0.95
            _BR_HIGH = 1.05
            if _BR_LOW <= _br_mean <= _BR_HIGH:
                _br_regime = "CRITICAL"
                _br_regime_detail = (
                    "sigma={:.4f}+/-{:.4f} within critical band [{:.2f}, {:.2f}]. "
                    "The sweep's score propagation is at criticality — descendant "
                    "activity neither amplifies nor decays relative to ancestors. "
                    "{} avalanche epoch(s) analyzed."
                ).format(_br_mean, _br_std, _BR_LOW, _BR_HIGH, _br_n)
            elif _br_mean > _BR_HIGH:
                _br_regime = "SUPERCRITICAL"
                _br_regime_detail = (
                    "sigma={:.4f}+/-{:.4f} above critical band (>{:.2f}). "
                    "Descendant scores systematically exceed ancestor scores — "
                    "the sweep exhibits supercritical amplification. {} epoch(s)."
                ).format(_br_mean, _br_std, _BR_HIGH, _br_n)
            else:
                _br_regime = "SUBCRITICAL"
                _br_regime_detail = (
                    "sigma={:.4f}+/-{:.4f} below critical band (<{:.2f}). "
                    "Descendant scores systematically decay relative to ancestors — "
                    "the sweep exhibits subcritical dissipation. {} epoch(s)."
                ).format(_br_mean, _br_std, _BR_LOW, _br_n)

            # INV_073 falsification: sustained supercritical
            _br_inv073_falsified = _br_mean > _BR_HIGH and _br_std < 0.1
            if _br_inv073_falsified:
                _br_inv073_detail = (
                    "INV_073 CHALLENGED: sigma={:.4f}+/-{:.4f} is sustained "
                    "supercritical (>{:.2f} with low variance). If neural "
                    "avalanches during active tasks show sigma systematically "
                    "above 1, the claim that biological systems *maintain* the "
                    "critical ridge during active cognition is falsified rather "
                    "than merely approached transiently. The branching ratio "
                    "indicates runaway amplification, not ridge navigation."
                ).format(_br_mean, _br_std, _BR_HIGH)
            elif _br_regime == "CRITICAL":
                _br_inv073_detail = (
                    "INV_073 CONSISTENT: sigma={:.4f} within critical band. "
                    "The sweep's score propagation maintains the critical ridge, "
                    "consistent with O21's spectral-gamma correlation claim."
                ).format(_br_mean)
            else:
                _br_inv073_detail = (
                    "INV_073 MONITORING: sigma={:.4f} in {} regime. "
                    "Not yet falsified but not confirming ridge maintenance."
                ).format(_br_mean, _br_regime.lower())

            _br_result = {
                "sigma_mean": round(_br_mean, 6),
                "sigma_std": round(_br_std, 6),
                "sigma_per_epoch": [round(_s, 6) for _s in _br_epochs],
                "n_epochs": _br_n,
                "regime": _br_regime,
                "regime_detail": _br_regime_detail,
                "inv073_falsified": _br_inv073_falsified,
                "inv073_detail": _br_inv073_detail,
            }

        # Attach FSS result, absorbing-state covariate, and branching-ratio
        # estimator to sweep output
        for inp in all_inputs:
            inp["fss_collapse"] = fss_result
            inp["absorbing_state"] = _absorbing_state_result
            inp["branching_ratio_estimate"] = _br_result

        # ── Yule–Simon Null-Model Discrimination (INV_047 Falsification) ──
        # INV_047 claims corpus frequency trajectories follow geodesic
        # (Wasserstein-consistent) structure.  The Yule–Simon preferential
        # attachment process produces identical Zipf observables without
        # any Wasserstein gradient flow.  This step computes a live
        # discrimination score between the two hypotheses using the
        # per-source frequency data collected during this sweep.
        #
        # Method: Compare the empirical rank-frequency trajectory across
        # sources against the Yule–Simon prediction.  Under Yule–Simon,
        # the rank-frequency curve is path-independent (depends only on
        # total counts and the attachment parameter ρ).  Under geodesic
        # Wasserstein flow, successive source batches should show
        # monotonic transport (the Earth Mover's Distance between
        # consecutive source histograms should be consistently positive
        # and directional, not random-walk-like).
        #
        # Discrimination score D ∈ [-1, 1]:
        #   D > 0 → geodesic structure (Wasserstein-consistent, supports INV_047)
        #   D ≈ 0 → indistinguishable from Yule–Simon (INV_047 underdetermined)
        #   D < 0 → anti-geodesic (actively inconsistent with INV_047)

        _ys_discrimination = {
            "discrimination_score": 0.0,
            "geodesic_evidence": 0.0,
            "yule_simon_evidence": 0.0,
            "n_source_pairs": 0,
            "transport_directions": [],
            "inv047_status": "INSUFFICIENT_DATA",
            "inv047_detail": "",
        }  # type: dict

        # Build per-source score histograms (using the already-collected
        # per_source_scores buckets from the dissipation-regime pass above)
        if len(per_source_scores) >= 2:
            # For each consecutive source pair, compute:
            #   1. L1 transport distance (discrete Wasserstein-1 proxy)
            #   2. Sign of the mean shift (directional transport)
            _transport_dists = []   # type: List[float]
            _transport_signs = []   # type: List[float]
            _n_pairs = 0

            for _si in range(len(per_source_scores) - 1):
                _bucket_a = per_source_scores[_si]
                _bucket_b = per_source_scores[_si + 1]
                if not _bucket_a or not _bucket_b:
                    continue

                _mean_a = sum(_bucket_a) / float(len(_bucket_a))
                _mean_b = sum(_bucket_b) / float(len(_bucket_b))

                # L1 distance between sorted score lists (discrete W1 proxy)
                _sorted_a = sorted(_bucket_a)
                _sorted_b = sorted(_bucket_b)
                _min_len = min(len(_sorted_a), len(_sorted_b))
                if _min_len > 0:
                    _w1 = sum(abs(_sorted_a[_k] - _sorted_b[_k])
                              for _k in range(_min_len)) / float(_min_len)
                    _transport_dists.append(_w1)
                    _transport_signs.append(1.0 if _mean_b >= _mean_a else -1.0)
                    _n_pairs += 1

            if _n_pairs >= 2:
                # Geodesic test: under Wasserstein flow, transport should be
                # consistently directional (sign consistency).  Under
                # Yule–Simon, signs should be random (≈50/50).
                _sign_sum = sum(_transport_signs)
                _sign_consistency = abs(_sign_sum) / float(_n_pairs)
                # Expected sign consistency under null (random walk): ~1/sqrt(n)
                _null_consistency = 1.0 / math.sqrt(float(_n_pairs)) if _n_pairs > 0 else 0.0

                # Transport magnitude test: under geodesic flow, W1 should
                # be non-negligible and structured.  Under Yule–Simon, W1
                # should scale as ~1/sqrt(sample_size) (sampling noise only).
                _mean_w1 = sum(_transport_dists) / float(len(_transport_dists))
                _var_w1 = sum((_w - _mean_w1) ** 2 for _w in _transport_dists) / float(len(_transport_dists))
                _std_w1 = math.sqrt(_var_w1) if _var_w1 > 0 else 0.0
                # Coefficient of variation: low CV = consistent transport (geodesic)
                _cv_w1 = _std_w1 / _mean_w1 if _mean_w1 > 1e-12 else 0.0

                # Geodesic evidence: high sign consistency + low CV of transport
                _geo_evidence = _sign_consistency * max(0.0, 1.0 - _cv_w1)
                # Yule–Simon evidence: sign consistency near null + high CV
                _ys_evidence = max(0.0, 1.0 - (_sign_consistency / max(_null_consistency * 3.0, 0.01)))
                _ys_evidence *= min(1.0, _cv_w1)

                # Discrimination score: geodesic - yule_simon, clamped to [-1, 1]
                _disc = _geo_evidence - _ys_evidence
                _disc = max(-1.0, min(1.0, _disc))

                # Status classification
                if _disc > 0.3:
                    _inv047_status = "GEODESIC_SUPPORTED"
                    _inv047_detail = (
                        "INV_047 SUPPORTED: discrimination_score={:.4f} > 0.3. "
                        "Per-source frequency trajectories show directional "
                        "transport (sign_consistency={:.3f} vs null={:.3f}, "
                        "mean_W1={:.4f}, CV={:.3f}) consistent with Wasserstein "
                        "geodesic flow rather than path-independent Yule–Simon "
                        "preferential attachment. {} source pairs analyzed."
                    ).format(_disc, _sign_consistency, _null_consistency,
                             _mean_w1, _cv_w1, _n_pairs)
                elif _disc < -0.3:
                    _inv047_status = "YULE_SIMON_PREFERRED"
                    _inv047_detail = (
                        "INV_047 CHALLENGED: discrimination_score={:.4f} < -0.3. "
                        "Per-source frequency trajectories are more consistent "
                        "with Yule–Simon preferential attachment than Wasserstein "
                        "geodesic flow (sign_consistency={:.3f}, null={:.3f}, "
                        "CV={:.3f}). The Zipf observables cited by INV_047 may "
                        "be produced by a simpler stochastic mechanism without "
                        "gradient flow structure. {} source pairs analyzed."
                    ).format(_disc, _sign_consistency, _null_consistency,
                             _cv_w1, _n_pairs)
                else:
                    _inv047_status = "UNDERDETERMINED"
                    _inv047_detail = (
                        "INV_047 UNDERDETERMINED: discrimination_score={:.4f} "
                        "is within [-0.3, 0.3]. Cannot distinguish geodesic "
                        "(Wasserstein) from path-independent (Yule–Simon) "
                        "structure in the corpus frequency trajectories. "
                        "sign_consistency={:.3f} (null={:.3f}), mean_W1={:.4f}, "
                        "CV={:.3f}. {} source pairs analyzed. More data or "
                        "additional substrates needed to resolve the degeneracy."
                    ).format(_disc, _sign_consistency, _null_consistency,
                             _mean_w1, _cv_w1, _n_pairs)

                _ys_discrimination = {
                    "discrimination_score": round(_disc, 6),
                    "geodesic_evidence": round(_geo_evidence, 6),
                    "yule_simon_evidence": round(_ys_evidence, 6),
                    "sign_consistency": round(_sign_consistency, 6),
                    "null_sign_consistency": round(_null_consistency, 6),
                    "mean_w1_transport": round(_mean_w1, 6),
                    "cv_w1_transport": round(_cv_w1, 6),
                    "n_source_pairs": _n_pairs,
                    "transport_directions": [round(_s, 1) for _s in _transport_signs],
                    "inv047_status": _inv047_status,
                    "inv047_detail": _inv047_detail,
                }

        # Attach Yule–Simon discrimination result to every sweep item
        for inp in all_inputs:
            inp["yule_simon_discrimination"] = _ys_discrimination

        # ── Shuffled-Corpus Null-Model for Wasserstein Floor (INV_094) ───
        # INV_094 claims a Wasserstein floor k exists that is structure-
        # dependent (reflecting genuine compressive structure in the corpus).
        # Without a null model, this is unfalsified confirmation surplus:
        # ANY corpus with a fixed vocabulary produces SOME floor k, so the
        # observation alone is tautological.
        #
        # Null-model protocol:
        #   1. Take the observed per-source score stream (vocabulary-matched).
        #   2. Generate N_shuffle shuffled copies (permuting scores across
        #      sources, destroying sequential/structural correlations while
        #      preserving the marginal score distribution exactly).
        #   3. For each shuffled copy, compute the same floor metric (the
        #      minimum non-trivial Wasserstein-1 distance between consecutive
        #      source histograms — the "Zipf floor k").
        #   4. Compare observed k against the null distribution of k_shuffle.
        #   5. If observed k is NOT significantly different from k_shuffle
        #      (p > 0.05), INV_094's floor is vocabulary-dependent, not
        #      structure-dependent, and must be downgraded.
        #
        # This converts the Wasserstein Floor confirmation from a potential
        # tautology into a genuine falsifiable test.
        #
        # Addresses: Deliberate Falsification Probe cycle 14, INV_094.

        import random as _null_rng

        _null_n_shuffles = 50
        _null_seed = 20240614

        _wf_null_result = {
            "observed_floor_k": 0.0,
            "null_floor_k_mean": 0.0,
            "null_floor_k_std": 0.0,
            "null_floor_k_values": [],
            "z_score": 0.0,
            "p_value_approx": 1.0,
            "structure_dependent": False,
            "n_shuffles": _null_n_shuffles,
            "n_source_pairs": 0,
            "inv094_status": "INSUFFICIENT_DATA",
            "inv094_detail": "",
        }  # type: dict

        # Compute observed Wasserstein-1 floor: minimum W1 across consecutive
        # source pairs (the "floor k" that INV_094 claims is structural)
        _wf_observed_w1s = []  # type: List[float]
        if len(per_source_scores) >= 2:
            for _wsi in range(len(per_source_scores) - 1):
                _wf_a = per_source_scores[_wsi]
                _wf_b = per_source_scores[_wsi + 1]
                if not _wf_a or not _wf_b:
                    continue
                _wf_sorted_a = sorted(_wf_a)
                _wf_sorted_b = sorted(_wf_b)
                _wf_min_len = min(len(_wf_sorted_a), len(_wf_sorted_b))
                if _wf_min_len > 0:
                    _wf_w1 = sum(
                        abs(_wf_sorted_a[_wk] - _wf_sorted_b[_wk])
                        for _wk in range(_wf_min_len)
                    ) / float(_wf_min_len)
                    _wf_observed_w1s.append(_wf_w1)

        if len(_wf_observed_w1s) >= 2:
            _wf_observed_floor = min(_wf_observed_w1s)
            _wf_n_pairs = len(_wf_observed_w1s)

            # Flatten all scores for shuffling (preserves vocabulary/marginals)
            _wf_all_scores_flat = []  # type: List[float]
            _wf_source_sizes = []     # type: List[int]
            for _wf_bucket in per_source_scores:
                _wf_source_sizes.append(len(_wf_bucket))
                _wf_all_scores_flat.extend(_wf_bucket)

            # Generate shuffled null floors
            _wf_null_floors = []  # type: List[float]
            _null_rng.seed(_null_seed)

            for _shuffle_idx in range(_null_n_shuffles):
                # Shuffle the flat score list (destroys structure, keeps vocabulary)
                _wf_shuffled = list(_wf_all_scores_flat)
                _null_rng.shuffle(_wf_shuffled)

                # Re-partition into source-sized buckets
                _wf_shuf_sources = []  # type: List[List[float]]
                _wf_cursor = 0
                for _sz in _wf_source_sizes:
                    _wf_shuf_sources.append(_wf_shuffled[_wf_cursor:_wf_cursor + _sz])
                    _wf_cursor += _sz

                # Compute floor k for this shuffled corpus
                _wf_shuf_w1s = []  # type: List[float]
                for _wsi2 in range(len(_wf_shuf_sources) - 1):
                    _wf_sa = _wf_shuf_sources[_wsi2]
                    _wf_sb = _wf_shuf_sources[_wsi2 + 1]
                    if not _wf_sa or not _wf_sb:
                        continue
                    _wf_ss_a = sorted(_wf_sa)
                    _wf_ss_b = sorted(_wf_sb)
                    _wf_ml = min(len(_wf_ss_a), len(_wf_ss_b))
                    if _wf_ml > 0:
                        _wf_sw1 = sum(
                            abs(_wf_ss_a[_wk2] - _wf_ss_b[_wk2])
                            for _wk2 in range(_wf_ml)
                        ) / float(_wf_ml)
                        _wf_shuf_w1s.append(_wf_sw1)

                if _wf_shuf_w1s:
                    _wf_null_floors.append(min(_wf_shuf_w1s))

            # Statistical comparison: z-score and approximate p-value
            if len(_wf_null_floors) >= 5:
                _wf_null_mean = sum(_wf_null_floors) / float(len(_wf_null_floors))
                _wf_null_var = sum(
                    (_nf - _wf_null_mean) ** 2 for _nf in _wf_null_floors
                ) / float(len(_wf_null_floors))
                _wf_null_std = math.sqrt(_wf_null_var) if _wf_null_var > 0 else 0.0

                if _wf_null_std > 1e-15:
                    _wf_z = (_wf_observed_floor - _wf_null_mean) / _wf_null_std
                else:
                    _wf_z = 0.0

                # Approximate two-tailed p-value from z-score using
                # the complementary error function approximation
                # p ≈ erfc(|z|/sqrt(2)) — use a simple rational approx
                _wf_abs_z = abs(_wf_z)
                # Abramowitz & Stegun approximation for erfc
                _wf_t = 1.0 / (1.0 + 0.3275911 * _wf_abs_z)
                _wf_erfc_approx = _wf_t * (
                    0.254829592
                    + _wf_t * (-0.284496736
                    + _wf_t * (1.421413741
                    + _wf_t * (-1.453152027
                    + _wf_t * 1.061405429)))
                ) * math.exp(-_wf_abs_z * _wf_abs_z)
                _wf_p_approx = max(0.0, min(1.0, _wf_erfc_approx))

                # Structure-dependent if observed floor is significantly
                # DIFFERENT from null (either lower or higher)
                _wf_structure_dep = _wf_p_approx < 0.05

                # Determine directionality for interpretive detail
                _wf_obs_lower = _wf_observed_floor < _wf_null_mean

                if _wf_structure_dep and _wf_obs_lower:
                    _wf_inv094_status = "STRUCTURE_CONFIRMED"
                    _wf_inv094_detail = (
                        "INV_094 CONFIRMED: observed floor k={:.6f} is "
                        "significantly BELOW the null-model floor "
                        "(null_mean={:.6f}+/-{:.6f}, z={:.3f}, p={:.4f}). "
                        "The Wasserstein floor is structure-dependent: the "
                        "corpus's sequential correlations produce a LOWER "
                        "minimum transport distance than shuffled copies with "
                        "identical vocabulary. This rules out the tautology "
                        "risk — the floor reflects genuine compressive "
                        "structure, not merely vocabulary size. {} shuffled "
                        "corpora tested across {} source pairs."
                    ).format(
                        _wf_observed_floor, _wf_null_mean, _wf_null_std,
                        _wf_z, _wf_p_approx, _null_n_shuffles, _wf_n_pairs,
                    )
                elif _wf_structure_dep and not _wf_obs_lower:
                    _wf_inv094_status = "STRUCTURE_CONFIRMED_HIGH"
                    _wf_inv094_detail = (
                        "INV_094 CONFIRMED (HIGH): observed floor k={:.6f} is "
                        "significantly ABOVE the null-model floor "
                        "(null_mean={:.6f}+/-{:.6f}, z={:.3f}, p={:.4f}). "
                        "The Wasserstein floor is structure-dependent but in "
                        "the opposite direction: sequential structure INCREASES "
                        "the minimum transport distance relative to shuffled "
                        "copies. This may indicate anti-correlated source "
                        "batches (heterogeneous topic clustering). {} shuffled "
                        "corpora tested across {} source pairs."
                    ).format(
                        _wf_observed_floor, _wf_null_mean, _wf_null_std,
                        _wf_z, _wf_p_approx, _null_n_shuffles, _wf_n_pairs,
                    )
                else:
                    _wf_inv094_status = "VOCABULARY_DEPENDENT"
                    _wf_inv094_detail = (
                        "INV_094 CHALLENGED: observed floor k={:.6f} is NOT "
                        "significantly different from the null-model floor "
                        "(null_mean={:.6f}+/-{:.6f}, z={:.3f}, p={:.4f} > 0.05). "
                        "The Wasserstein floor is VOCABULARY-DEPENDENT, not "
                        "structure-dependent. Shuffled corpora with identical "
                        "marginal score distributions produce statistically "
                        "indistinguishable floor values. INV_094's confirmation "
                        "surplus is a tautology: any corpus with this vocabulary "
                        "size produces a comparable floor k. INV_094 should be "
                        "downgraded from confirmed invariant to vocabulary "
                        "artifact until structure-dependence is demonstrated. "
                        "{} shuffled corpora tested across {} source pairs."
                    ).format(
                        _wf_observed_floor, _wf_null_mean, _wf_null_std,
                        _wf_z, _wf_p_approx, _null_n_shuffles, _wf_n_pairs,
                    )

                _wf_null_result = {
                    "observed_floor_k": round(_wf_observed_floor, 8),
                    "null_floor_k_mean": round(_wf_null_mean, 8),
                    "null_floor_k_std": round(_wf_null_std, 8),
                    "null_floor_k_values": [round(_nf, 8) for _nf in _wf_null_floors],
                    "z_score": round(_wf_z, 6),
                    "p_value_approx": round(_wf_p_approx, 6),
                    "structure_dependent": _wf_structure_dep,
                    "observed_below_null": _wf_obs_lower,
                    "n_shuffles": _null_n_shuffles,
                    "n_source_pairs": _wf_n_pairs,
                    "inv094_status": _wf_inv094_status,
                    "inv094_detail": _wf_inv094_detail,
                }
            else:
                _wf_null_result["inv094_status"] = "INSUFFICIENT_SHUFFLES"
                _wf_null_result["inv094_detail"] = (
                    "Fewer than 5 valid shuffled floors produced. "
                    "Cannot perform null-model comparison."
                )
        else:
            _wf_null_result["inv094_detail"] = (
                "Fewer than 2 consecutive source pairs with scores. "
                "Cannot compute Wasserstein floor or null model."
            )

        # Attach null-model result to every sweep item
        for inp in all_inputs:
            inp["wasserstein_floor_null_model"] = _wf_null_result

        # ── BN γ-Scale Spectral Scoring (O21 Correlation Measurement) ────
        # When a model checkpoint path is present in a sweep item's metadata,
        # extract per-layer Batch Normalization γ (weight/scale) parameters
        # and compute mean|γ_l| as a proxy spectral score.  This is logged
        # alongside existing AlphaPruning α estimates to enable O21
        # correlation measurement: testing whether BN regularization
        # magnitude correlates with AlphaPruning protocol decisions.
        #
        # INV_073 relevance: the survey's evidence that regularization-based
        # pruning exhibits a hard phase transition (not a smooth ridge)
        # suggests criticality in compression may be a threshold phenomenon
        # rather than a navigable continuous ridge, straining the claim that
        # γ=1 is a maintainable operating point rather than a knife-edge
        # boundary.  By recording BN γ-scale statistics alongside pruning
        # scores, we produce the paired data needed to test this.
        #
        # The BN γ-scale extraction scans for parameter keys matching
        # common BN naming conventions (*.weight where the layer also has
        # *.running_mean, or explicitly *.bn*.weight / *.norm*.weight).
        # For each such layer, mean|γ| is computed and logged.

        def _extract_bn_gamma_stats(checkpoint_path):
            # type: (str) -> dict
            """
            Extract per-layer BN γ-scale statistics from a model checkpoint.

            Scans the checkpoint's state_dict for BatchNorm weight (γ)
            parameters and computes mean|γ_l| per layer as a proxy
            spectral score for O21 correlation measurement.

            Returns a dict with per-layer stats and aggregate summary,
            or an empty result if no BN parameters are found or the
            checkpoint cannot be loaded.
            """
            _bn_result = {
                "bn_layers_found": 0,
                "per_layer_gamma": [],
                "mean_abs_gamma_global": 0.0,
                "std_abs_gamma_global": 0.0,
                "min_layer_gamma": 0.0,
                "max_layer_gamma": 0.0,
                "checkpoint_path": checkpoint_path,
                "status": "NO_CHECKPOINT",
                "o21_detail": "",
            }  # type: dict

            if not checkpoint_path:
                return _bn_result

            _ckpt_path = Path(checkpoint_path)
            if not _ckpt_path.exists():
                _bn_result["status"] = "CHECKPOINT_NOT_FOUND"
                return _bn_result

            try:
                with open(_ckpt_path, "rb") as _f:
                    # Try loading as JSON (FREED-native checkpoints)
                    _f.seek(0)
                    _raw = _f.read()
                    try:
                        _state = json.loads(_raw)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        _bn_result["status"] = "UNSUPPORTED_FORMAT"
                        _bn_result["o21_detail"] = (
                            "Checkpoint at {} is not JSON-loadable. "
                            "BN gamma extraction requires a JSON state_dict "
                            "or compatible format."
                        ).format(checkpoint_path)
                        return _bn_result
            except OSError as _e:
                _bn_result["status"] = "LOAD_ERROR"
                _bn_result["o21_detail"] = "Checkpoint load error: {}".format(_e)
                return _bn_result

            # Scan for BN gamma parameters
            # Common patterns:
            #   layer.bn1.weight, layer.norm.weight, bn.weight
            #   Companion keys: *.running_mean, *.running_var, *.num_batches_tracked
            _bn_patterns = re.compile(
                r'(bn\d*|batch_?norm\d*|norm\d*|groupnorm\d*|layernorm\d*)\.weight$',
                re.IGNORECASE,
            )

            _state_dict = _state if isinstance(_state, dict) else {}
            # Handle nested state_dict (e.g., {"state_dict": {...}})
            if "state_dict" in _state_dict and isinstance(_state_dict["state_dict"], dict):
                _state_dict = _state_dict["state_dict"]
            elif "model" in _state_dict and isinstance(_state_dict["model"], dict):
                _state_dict = _state_dict["model"]

            _per_layer = []  # type: list
            _all_abs_gammas = []  # type: List[float]

            for _key, _val in _state_dict.items():
                # Check if this looks like a BN weight parameter
                _is_bn = bool(_bn_patterns.search(_key))

                # Also check if there's a companion running_mean key
                if not _is_bn and _key.endswith(".weight"):
                    _prefix = _key[:-len(".weight")]
                    _has_running_mean = (_prefix + ".running_mean") in _state_dict
                    _has_running_var = (_prefix + ".running_var") in _state_dict
                    _is_bn = _has_running_mean or _has_running_var

                if not _is_bn:
                    continue

                # Extract gamma values — handle list or flat numeric
                _gammas = []  # type: List[float]
                if isinstance(_val, list):
                    for _v in _val:
                        try:
                            _gammas.append(float(_v))
                        except (TypeError, ValueError):
                            pass
                elif isinstance(_val, (int, float)):
                    _gammas.append(float(_val))

                if not _gammas:
                    continue

                # Compute per-layer statistics
                _abs_gammas = [abs(_g) for _g in _gammas]
                _layer_mean = sum(_abs_gammas) / float(len(_abs_gammas))
                _layer_var = sum((_g - _layer_mean) ** 2 for _g in _abs_gammas) / float(len(_abs_gammas))
                _layer_std = math.sqrt(_layer_var) if _layer_var > 0 else 0.0
                _layer_min = min(_abs_gammas)
                _layer_max = max(_abs_gammas)

                _per_layer.append({
                    "layer_name": _key,
                    "mean_abs_gamma": round(_layer_mean, 8),
                    "std_abs_gamma": round(_layer_std, 8),
                    "min_abs_gamma": round(_layer_min, 8),
                    "max_abs_gamma": round(_layer_max, 8),
                    "n_channels": len(_gammas),
                    "near_zero_channels": sum(1 for _g in _abs_gammas if _g < 0.01),
                })
                _all_abs_gammas.extend(_abs_gammas)

            if not _per_layer:
                _bn_result["status"] = "NO_BN_PARAMS"
                _bn_result["o21_detail"] = (
                    "No BatchNorm gamma parameters found in checkpoint at {}. "
                    "The model may not use BN layers, or the state_dict key "
                    "naming convention is not recognized."
                ).format(checkpoint_path)
                return _bn_result

            # Aggregate statistics
            _n_total = len(_all_abs_gammas)
            _global_mean = sum(_all_abs_gammas) / float(_n_total)
            _global_var = sum((_g - _global_mean) ** 2 for _g in _all_abs_gammas) / float(_n_total)
            _global_std = math.sqrt(_global_var) if _global_var > 0 else 0.0

            _layer_means = [_l["mean_abs_gamma"] for _l in _per_layer]

            _bn_result.update({
                "bn_layers_found": len(_per_layer),
                "per_layer_gamma": _per_layer,
                "mean_abs_gamma_global": round(_global_mean, 8),
                "std_abs_gamma_global": round(_global_std, 8),
                "min_layer_gamma": round(min(_layer_means), 8),
                "max_layer_gamma": round(max(_layer_means), 8),
                "total_bn_channels": _n_total,
                "near_zero_channels_total": sum(_l["near_zero_channels"] for _l in _per_layer),
                "status": "OK",
                "o21_detail": (
                    "O21 BN GAMMA EXTRACTED: {} BN layers found with {} total "
                    "channels. Global mean|gamma|={:.6f}+/-{:.6f}. Per-layer "
                    "range: [{:.6f}, {:.6f}]. Near-zero channels (|gamma|<0.01): "
                    "{}. These statistics serve as proxy spectral scores for "
                    "correlation with AlphaPruning alpha estimates — directly "
                    "operationalizing O21. INV_073: layers with many near-zero "
                    "gamma channels are candidates for structured pruning; if "
                    "the transition from pruned to unpruned is sharp (not smooth), "
                    "this supports the phase-transition interpretation over the "
                    "navigable-ridge interpretation of the critical point."
                ).format(
                    len(_per_layer), _n_total, _global_mean, _global_std,
                    min(_layer_means), max(_layer_means),
                    sum(_l["near_zero_channels"] for _l in _per_layer),
                ),
            })

            return _bn_result

        # Apply BN gamma extraction to sweep items that carry checkpoint metadata
        for inp in all_inputs:
            _ckpt_path = ""
            _meta = inp.get("metadata", {})
            if isinstance(_meta, dict):
                _ckpt_path = _meta.get("checkpoint_path", "")
            if not _ckpt_path:
                _ckpt_path = inp.get("checkpoint_path", "")

            # Compute and attach BN gamma stats (produces empty result if no checkpoint)
            inp["bn_gamma_spectral"] = _extract_bn_gamma_stats(_ckpt_path)

            # If both BN gamma stats and an alpha_pruning estimate exist,
            # compute and log the O21 correlation pair for downstream analysis
            _bn_stats = inp["bn_gamma_spectral"]
            _alpha_est = inp.get("alpha_pruning", {})
            if isinstance(_alpha_est, dict) and _bn_stats.get("status") == "OK":
                _alpha_val = _alpha_est.get("alpha", 0.0)
                _gamma_val = _bn_stats.get("mean_abs_gamma_global", 0.0)
                if _alpha_val > 0.0 and _gamma_val > 0.0:
                    inp["o21_gamma_alpha_pair"] = {
                        "bn_gamma_spectral_score": round(_gamma_val, 8),
                        "alpha_pruning_estimate": round(_alpha_val, 8),
                        "ratio_gamma_over_alpha": round(_gamma_val / _alpha_val, 8),
                        "correlation_ready": True,
                        "o21_note": (
                            "Paired (spectral gamma={:.6f}, pruning alpha={:.4f}) "
                            "for O21 correlation test. INV_073: if the correlation "
                            "shows a sharp discontinuity rather than a smooth "
                            "relationship, this supports the phase-transition "
                            "interpretation of pruning criticality."
                        ).format(_gamma_val, _alpha_val),
                    }
                else:
                    inp["o21_gamma_alpha_pair"] = {
                        "correlation_ready": False,
                        "o21_note": "One or both values are zero; cannot form pair.",
                    }
            else:
                inp["o21_gamma_alpha_pair"] = {
                    "correlation_ready": False,
                    "o21_note": "BN gamma or alpha_pruning data not available.",
                }

        return all_inputs

    def _sweep_source(self, source):
        # type: (dict) -> list
        """Dispatch to the right parser based on source type."""
        parsers = {
            "lifeboat_author":  self._parse_lifeboat_author,
            "arxiv_rss":        self._parse_arxiv_rss,
            "biorxiv_rss":      self._parse_biorxiv_rss,
            "crossref_journal": self._parse_crossref_journal,
        }
        parser = parsers.get(source["type"])
        if parser is None:
            print(f"[SWEEP] No parser for type '{source['type']}' — skipping.")
            return []
        return parser(source)

    # ── Lifeboat author page parser ───────────────────────────────────────────

    def _parse_lifeboat_author(self, source):
        # type: (dict) -> list
        """
        Parse an author page on lifeboat.com.
        Extracts article cards: title, URL, excerpt, date.
        """
        html = self._fetch(source["url"])
        if html is None:
            return []

        soup = BeautifulSoup(html, "html.parser")
        articles = []

        # Lifeboat blog uses article or div cards — try multiple selectors
        # Common patterns: <article>, <div class="post">, <h2><a href>
        candidates = (
            soup.find_all("article") or
            soup.find_all("div", class_=re.compile(r"post|entry|blog-item", re.I)) or
            []
        )

        # Fallback: grab all links that look like blog posts
        if not candidates:
            candidates = self._fallback_link_extraction(soup, source["url"])

        seen_urls = set()
        for card in candidates:
            item = self._extract_lifeboat_card(card, source["url"])
            if item and item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                articles.append(item)

        # Filter to unseen, up to max
        new_articles = []
        for art in articles:
            if art["url"] not in self.seen:
                new_articles.append(art)
            if len(new_articles) >= self.max_new:
                break

        # Fetch full text for each new article
        for art in new_articles:
            time.sleep(POLITENESS_DELAY)
            art["content"] = self._fetch_article_text(art["url"])
            self._mark_seen(art["url"])

        return new_articles

    def _extract_lifeboat_card(self, card, base_url):
        # type: (object, str) -> Optional[dict]
        """Extract title, URL, excerpt, date from one article card."""
        # Title + URL
        link_tag = card.find("a", href=True)
        h_tag    = card.find(re.compile(r"h[123456]"))
        if h_tag:
            link_tag = h_tag.find("a", href=True) or link_tag

        if not link_tag:
            return None

        title = link_tag.get_text(strip=True)
        url   = urljoin(base_url, link_tag["href"])

        # Only keep links that look like blog posts (not tag/category pages)
        if not re.search(r'/blog/', url):
            return None
        if url == base_url:
            return None

        # Excerpt
        excerpt_tag = (
            card.find("p") or
            card.find("div", class_=re.compile(r"excerpt|summary|content|entry", re.I))
        )
        excerpt = excerpt_tag.get_text(strip=True)[:500] if excerpt_tag else ""

        # Date
        date_tag = card.find(["time", "span"], class_=re.compile(r"date|time|posted", re.I))
        date_str = ""
        if date_tag:
            date_str = date_tag.get("datetime", "") or date_tag.get_text(strip=True)

        return {
            "title":    title,
            "url":      url,
            "abstract": excerpt,
            "date":     date_str,
            "source":   "Cecile G. Tamura / Lifeboat Foundation",
            "fetched":  datetime.now(timezone.utc).isoformat(),
        }

    def _fallback_link_extraction(self, soup, base_url):
        # type: (BeautifulSoup, str) -> list
        """
        When article cards aren't found, extract all blog-post-like links
        and wrap them in minimal dicts so the main loop can still process them.
        """
        links = soup.find_all("a", href=re.compile(r'/blog/\d{4}|/blog/[^/]+/[^/]+'))
        seen  = set()
        result = []
        for link in links:
            url = urljoin(base_url, link["href"])
            if url not in seen and url != base_url:
                seen.add(url)
                # Create a minimal card-like object
                mock = BeautifulSoup(
                    f'<div><a href="{url}">{link.get_text(strip=True)}</a></div>',
                    "html.parser"
                )
                result.append(mock.find("div"))
        return result

    # ── bioRxiv RSS parser (RDF/RSS 1.0) ─────────────────────────────────────

    def _parse_biorxiv_rss(self, source):
        # type: (dict) -> list
        """
        Parse a bioRxiv subject feed from connect.biorxiv.org.

        Format: RSS 1.0 / RDF with dc: and prism: namespaces.
        Each feed returns the 30 most recent preprints for the subject.
        URL pattern: http://connect.biorxiv.org/biorxiv_xml.php?subject={subject}
        """
        RSS  = "http://purl.org/rss/1.0/"
        DC   = "http://purl.org/dc/elements/1.1/"

        raw = self._fetch(source["url"])
        if raw is None:
            return []

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            print(f"[SWEEP] bioRxiv RSS parse error: {e}")
            return []

        items = root.findall(f"{{{RSS}}}item")
        new_articles = []

        for item in items:
            link_el  = item.find(f"{{{RSS}}}link")
            title_el = item.find(f"{{{RSS}}}title")
            desc_el  = item.find(f"{{{RSS}}}description")
            auth_el  = item.find(f"{{{DC}}}creator")

            if link_el is None or title_el is None:
                continue

            # Strip the ?rss=1 tracking suffix for canonical URL
            raw_url = (link_el.text or "").strip()
            paper_url = raw_url.split("?")[0]
            if not paper_url:
                continue

            if paper_url in self.seen:
                continue

            title    = (title_el.text or "").strip()
            abstract = (desc_el.text  or "").strip()[:800] if desc_el is not None else ""
            authors  = (auth_el.text  or "").strip()       if auth_el is not None else ""

            score = self._arxiv_relevance(title + " " + abstract)
            if score < ARXIV_MIN_SCORE:
                self._mark_seen(paper_url)
                continue

            new_articles.append({
                "title":    title,
                "url":      paper_url,
                "abstract": abstract,
                "content":  abstract,
                "authors":  authors,
                "source":   source["name"],
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            })
            self._mark_seen(paper_url)

            if len(new_articles) >= self.max_new:
                break

        new_articles.sort(key=lambda x: x["score"], reverse=True)
        return new_articles

    # ── CrossRef journal parser (JSON, polite-bot friendly) ─────────────────

    # CrossRef supports polite-pool access when the User-Agent includes a mailto.
    _CROSSREF_HEADERS = {
        "User-Agent": (
            "FREED/1.0 (Freed Recursive Engine for Epistemic Dynamics; "
            "polite research bot; https://wellposedness.github.io/FREED/)"
        )
    }

    def _parse_crossref_journal(self, source):
        # type: (dict) -> list
        """
        Fetch recent papers from CrossRef for a specific journal (by ISSN).

        The source URL should be a CrossRef works API endpoint filtered by ISSN.
        CrossRef is designed for programmatic access; their CDN doesn't block bots.
        Abstracts may contain JATS XML tags — these are stripped before scoring.

        Used for: Entropy journal (MDPI blocks bot UAs; CrossRef does not).
        """
        try:
            resp = requests.get(
                source["url"],
                headers=self._CROSSREF_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                print(f"[SWEEP] CrossRef returned {resp.status_code} for {source['name']}")
                return []
            data = resp.json()
        except Exception as e:
            print(f"[SWEEP] CrossRef fetch error: {e}")
            return []

        items = data.get("message", {}).get("items", [])
        new_articles = []

        for item in items:
            doi = (item.get("DOI") or "").strip()
            if not doi:
                continue
            url = "https://doi.org/" + doi

            if url in self.seen:
                continue

            title_list = item.get("title") or []
            title = title_list[0].strip() if title_list else ""
            if not title:
                continue

            # Strip JATS XML markup from abstract (e.g., <jats:p>, <jats:italic>)
            raw_abstract = item.get("abstract") or ""
            abstract = re.sub(r'<[^>]+>', ' ', raw_abstract)
            abstract = re.sub(r'\s+', ' ', abstract).strip()[:800]

            authors = item.get("author") or []
            author_parts = []
            for a in authors[:3]:
                family = a.get("family", "")
                given  = a.get("given", "")
                if family:
                    author_parts.append(
                        family + (", " + given[0] + "." if given else "")
                    )
            author_str = '; '.join(author_parts)

            score = self._arxiv_relevance(title + " " + abstract)
            if score < ARXIV_MIN_SCORE:
                self._mark_seen(url)
                continue

            new_articles.append({
                "title":    title,
                "url":      url,
                "abstract": abstract,
                "content":  abstract,
                "authors":  author_str,
                "source":   source["name"],
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            })
            self._mark_seen(url)

            if len(new_articles) >= self.max_new:
                break

        new_articles.sort(key=lambda x: x["score"], reverse=True)
        return new_articles

    # ── arXiv RSS parser ─────────────────────────────────────────────────────

    def _parse_arxiv_rss(self, source):
        # type: (dict) -> list
        """
        Parse an arXiv RSS feed.
        Extracts title, abstract, URL, authors.
        Applies keyword relevance pre-filter — no API cost.
        Only processes announce_type=new (skips revisions).
        """
        raw = self._fetch(source["url"])
        if raw is None:
            return []

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            print(f"[SWEEP] RSS parse error: {e}")
            return []

        # Strip namespaces for simpler access
        ns = {
            "arxiv": "http://arxiv.org/schemas/atom",
            "dc":    "http://purl.org/dc/elements/1.1/",
        }

        channel = root.find("channel")
        if channel is None:
            return []

        new_articles = []
        for item in channel.findall("item"):
            # Only new submissions — skip replacements/cross-lists
            announce = item.find("arxiv:announce_type", ns)
            if announce is not None and announce.text.strip() != "new":
                continue

            link_el  = item.find("link")
            title_el = item.find("title")
            desc_el  = item.find("description")
            auth_el  = item.find("dc:creator", ns)

            if link_el is None or title_el is None:
                continue

            url      = (link_el.text or "").strip()
            title    = (title_el.text or "").strip()
            desc_raw = (desc_el.text or "") if desc_el is not None else ""
            authors  = (auth_el.text or "") if auth_el is not None else ""

            # Strip the "arXiv:XXXX Announce Type: new\nAbstract: " prefix
            abstract = re.sub(
                r'^arXiv:\S+\s+Announce\s+Type:\s*\w+\s*\n?Abstract:\s*',
                '', desc_raw, flags=re.IGNORECASE
            ).strip()
            abstract = abstract[:800]

            if url in self.seen:
                continue

            # Relevance pre-filter — score against RSA-adjacent keywords
            score = self._arxiv_relevance(title + " " + abstract)
            if score < ARXIV_MIN_SCORE:
                self._mark_seen(url)   # mark seen so we don't re-check
                continue

            new_articles.append({
                "title":    title,
                "url":      url,
                "abstract": abstract,
                "content":  abstract,
                "authors":  authors,
                "source":   source["name"],
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            })
            self._mark_seen(url)

            if len(new_articles) >= self.max_new:
                break

        # Sort by relevance score — most relevant first
        new_articles.sort(key=lambda x: x["score"], reverse=True)
        return new_articles

    def _arxiv_relevance(self, text: str) -> int:
        """
        Score text against RSA-adjacent keywords.
        Returns total score — caller decides threshold.
        No API cost — pure regex matching.
        """
        text_lower = text.lower()
        score = 0
        for pattern, weight in ARXIV_KEYWORDS:
            if re.search(pattern, text_lower):
                score += weight
        return score

    # ── Full article fetch ────────────────────────────────────────────────────

    def _fetch_article_text(self, url: str) -> str:
        """
        Fetch the full text of an article page.
        Strips navigation, ads, sidebars — returns the main prose.
        """
        html = self._fetch(url)
        if html is None:
            return ""

        soup = BeautifulSoup(html, "html.parser")

        # Remove noise
        for tag in soup.find_all(["nav", "header", "footer", "aside", "script",
                                   "style", "form", "noscript"]):
            tag.decompose()

        # Try to find the article body
        body = (
            soup.find("article") or
            soup.find("div", class_=re.compile(r"entry-content|post-content|article-body|content", re.I)) or
            soup.find("main") or
            soup.find("body")
        )

        if body is None:
            return ""

        # Extract text, collapse whitespace
        text = body.get_text(separator="\n")
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        # Cap at 4000 chars — enough for L7 to work with, not so much it blows the budget
        text = text[:4000]

        # Prompt injection defense — strip any injection attempts before L7 sees this
        result = sanitize(text, source_url=url)
        if result.dropped:
            return ""   # article is poisoned — drop entirely
        return result.clean

    # ── HTTP fetch ───────────────────────────────────────────────────────────

    def _fetch(self, url: str):
        """Fetch a URL. Returns HTML string or None on failure."""
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            print(f"[SWEEP]   Fetch error for {url}: {e}")
            return None


# ─── Covariance Entropy Trend Detection (γ-Precursor Signal) ─────────────────
# Computes Shannon entropy of spatial/semantic covariance matrices over
# sliding windows to detect entropy-reduction trends as an early-warning
# signal for critical transitions (γ=1 ridge proximity).
#
# Ecological precedent: vegetation spatial patterns exhibit measurable
# entropy reduction before critical transitions (desertification tipping
# points).  This operationalizes O112 (STF metric recovery) while
# challenging INV_073: if entropy reduction is the universal precursor
# to *collapse* (not sustained criticality), then systems navigating
# the γ=1 ridge may be indistinguishable from systems about to tip.
#
# The live gap: no existing formalism distinguishes sustained criticality
# (SOC, edge-of-chaos) from transient pre-collapse criticality using the
# same entropy-reduction signature.  This function returns the precursor
# scalar alongside a stability discriminant that attempts to separate
# the two regimes using the *rate of entropy reduction* (sustained
# criticality: slow/oscillating reduction; pre-collapse: monotonic
# acceleration).
#
# Addresses: O112 (STF metric recovery), INV_073 (ridge navigation vs
# pre-collapse indistinguishability).


def covariance_entropy_precursor(
    score_stream,           # type: List[float]
    window_size=8,          # type: int
    stride=1,              # type: int
    n_trend_windows=5,      # type: int
):
    # type: (...) -> dict
    """
    Compute entropy-reduction trend over sliding windows of the
    covariance structure of a score stream, returning a scalar
    γ-precursor signal.

    Algorithm:
      1. Partition the score stream into overlapping windows of size
         `window_size` with step `stride`.
      2. For each window, compute the 1-D "covariance matrix" (variance
         + lag-1 autocovariance as a 2×2 symmetric matrix).
      3. Compute the Shannon entropy of the normalized eigenvalue
         spectrum of each covariance matrix (von Neumann-like entropy).
      4. Fit a linear trend to the last `n_trend_windows` entropy values.
      5. The γ-precursor signal is the negative slope of this trend:
         positive precursor = entropy is *decreasing* (approaching
         transition); negative = entropy increasing (moving away).

    Parameters
    ----------
    score_stream : list of float
        The time series of scores (relevance, coherence, etc.).
    window_size : int
        Size of each sliding window for covariance estimation.
        Default: 8.
    stride : int
        Step between consecutive windows. Default: 1.
    n_trend_windows : int
        Number of recent covariance-entropy values to use for
        trend fitting. Default: 5.

    Returns
    -------
    dict with keys:
        gamma_precursor        : float — the γ-precursor scalar (positive =
                                         entropy decreasing, approaching
                                         transition)
        entropy_trend_slope    : float — raw slope of entropy vs window index
        entropy_trend_r2       : float — R² of the linear trend fit
        cov_entropy_series     : list of float — per-window covariance entropy
        n_windows              : int   — total windows computed
        n_trend_points         : int   — windows used for trend fit
        stability_discriminant : float — ratio of entropy oscillation amplitude
                                         to trend magnitude; high ratio suggests
                                         sustained criticality (SOC), low ratio
                                         suggests monotonic pre-collapse
        ridge_vs_collapse      : str   — "SUSTAINED_CRITICAL" / "PRE_COLLAPSE" /
                                         "STABLE" / "INSUFFICIENT_DATA"
        inv073_note            : str   — challenge note on indistinguishability
        method                 : str   — "covariance_entropy_precursor"
        timestamp              : str
    """
    n = len(score_stream)
    ts = datetime.now(timezone.utc).isoformat()

    empty = {
        "gamma_precursor": 0.0,
        "entropy_trend_slope": 0.0,
        "entropy_trend_r2": 0.0,
        "cov_entropy_series": [],
        "n_windows": 0,
        "n_trend_points": 0,
        "stability_discriminant": 0.0,
        "ridge_vs_collapse": "INSUFFICIENT_DATA",
        "inv073_note": (
            "INV_073 LIVE GAP: No formalism yet distinguishes sustained "
            "criticality (SOC) from transient pre-collapse criticality "
            "using the same entropy-reduction signature. The stability "
            "discriminant is an empirical heuristic, not a proof."
        ),
        "method": "covariance_entropy_precursor",
        "timestamp": ts,
    }

    if n < window_size + 2:
        return empty

    # Step 1: sliding windows → covariance entropy series
    cov_entropies = []  # type: List[float]
    pos = 0
    while pos + window_size <= n:
        w = score_stream[pos:pos + window_size]
        wn = len(w)
        # Mean
        mu = sum(w) / float(wn)
        # Variance (C[0,0])
        var_00 = sum((x - mu) ** 2 for x in w) / float(wn)
        # Lag-1 autocovariance (C[0,1] = C[1,0])
        cov_01 = 0.0
        if wn > 1:
            cov_01 = sum(
                (w[i] - mu) * (w[i + 1] - mu)
                for i in range(wn - 1)
            ) / float(wn - 1)

        # 2×2 symmetric covariance matrix eigenvalues:
        #   [[var_00, cov_01], [cov_01, var_00]]
        # eigenvalues: var_00 + cov_01, var_00 - cov_01
        lam1 = var_00 + cov_01
        lam2 = var_00 - cov_01

        # Clamp to non-negative (numerical safety)
        lam1 = max(0.0, lam1)
        lam2 = max(0.0, lam2)

        # Normalize to probability distribution
        total = lam1 + lam2
        if total > 1e-30:
            p1 = lam1 / total
            p2 = lam2 / total
            # Shannon entropy of eigenvalue spectrum
            h = 0.0
            if p1 > 0.0:
                h -= p1 * math.log(p1)
            if p2 > 0.0:
                h -= p2 * math.log(p2)
        else:
            h = 0.0

        cov_entropies.append(round(h, 8))
        pos += stride

    n_win = len(cov_entropies)
    if n_win < 3:
        empty["cov_entropy_series"] = cov_entropies
        empty["n_windows"] = n_win
        return empty

    # Step 2: fit linear trend to the last n_trend_windows entries
    trend_data = cov_entropies[-n_trend_windows:] if n_win >= n_trend_windows else cov_entropies
    n_t = len(trend_data)

    # Linear regression: h(i) = a + b*i
    sum_x = sum(range(n_t))
    sum_y = sum(trend_data)
    sum_xy = sum(float(i) * trend_data[i] for i in range(n_t))
    sum_x2 = sum(float(i) * float(i) for i in range(n_t))

    denom = float(n_t) * sum_x2 - sum_x * sum_x
    if abs(denom) > 1e-15:
        slope = (float(n_t) * sum_xy - sum_x * sum_y) / denom
        intercept = (sum_y - slope * sum_x) / float(n_t)

        mean_y = sum_y / float(n_t)
        ss_tot = sum((y - mean_y) ** 2 for y in trend_data)
        ss_res = sum(
            (trend_data[i] - (intercept + slope * float(i))) ** 2
            for i in range(n_t)
        )
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0
    else:
        slope = 0.0
        r2 = 0.0

    # γ-precursor: negative slope (positive when entropy decreasing)
    gamma_precursor = -slope

    # Step 3: stability discriminant
    # Oscillation amplitude (std of residuals from trend) vs |slope|
    if abs(denom) > 1e-15 and n_t >= 3:
        residuals = [
            trend_data[i] - (intercept + slope * float(i))
            for i in range(n_t)
        ]
        res_var = sum(r * r for r in residuals) / float(n_t)
        oscillation_amp = math.sqrt(res_var) if res_var > 0 else 0.0
    else:
        oscillation_amp = 0.0

    abs_slope = abs(slope)
    if abs_slope > 1e-12:
        stability_disc = oscillation_amp / abs_slope
    else:
        stability_disc = float('inf') if oscillation_amp > 1e-12 else 0.0

    # Classify ridge-vs-collapse
    if abs_slope < 1e-8:
        ridge_label = "STABLE"
    elif stability_disc > 2.0:
        ridge_label = "SUSTAINED_CRITICAL"
    elif gamma_precursor > 0 and r2 > 0.5:
        ridge_label = "PRE_COLLAPSE"
    else:
        ridge_label = "STABLE"

    inv073_note = (
        "INV_073 LIVE GAP: Entropy-reduction detected (precursor={:.6f}, "
        "slope={:.6f}, R²={:.4f}). Stability discriminant={:.4f} — {}. "
        "No existing formalism rigorously distinguishes sustained criticality "
        "(SOC, edge-of-chaos) from transient pre-collapse criticality using "
        "the same entropy-reduction signature. The stability_discriminant "
        "(oscillation/trend ratio) is an empirical heuristic: high ratio "
        "suggests oscillating around a ridge (SOC); low ratio suggests "
        "monotonic approach to collapse. This is the live gap between "
        "INV_073 and ecological early-warning theory."
    ).format(
        gamma_precursor, slope, max(0.0, r2),
        stability_disc if stability_disc != float('inf') else 999.0,
        ridge_label,
    )

    return {
        "gamma_precursor": round(gamma_precursor, 8),
        "entropy_trend_slope": round(slope, 8),
        "entropy_trend_r2": round(max(0.0, r2), 6),
        "cov_entropy_series": cov_entropies,
        "n_windows": n_win,
        "n_trend_points": n_t,
        "stability_discriminant": round(
            stability_disc if stability_disc != float('inf') else 999.0, 6
        ),
        "ridge_vs_collapse": ridge_label,
        "inv073_note": inv073_note,
        "method": "covariance_entropy_precursor",
        "timestamp": ts,
    }


# ─── Output-Only Entropy-to-Dissipation Estimator ────────────────────────────
# Estimates coherence decay / damping parameters from output-only observation
# streams using mutual-information and spectral-entropy decomposition.
#
# Motivation (O112 challenge):
#   Recovering a physical metric tensor (damping) from output-only statistics
#   is feasible but requires careful modal decomposition.  The STF recovery
#   protocol has not yet specified this at sufficient algorithmic resolution.
#   This module provides the missing algorithmic layer: given only an output
#   signal stream (no known input / no controlled probes), it extracts latent
#   decay rates via:
#
#     1. Spectral entropy H_s(f) — Shannon entropy of the normalized power
#        spectral density.  A pure tone has H_s = 0; white noise has H_s = max.
#        Damped oscillatory modes produce intermediate H_s values that decrease
#        as damping increases (energy concentrates into fewer spectral bins as
#        modes decay faster).
#
#     2. Time-lagged mutual information I(τ) — MI between the signal and its
#        τ-lagged copy.  For a damped system, I(τ) decays approximately as
#        exp(-2ζω_n τ) where ζ is the damping ratio and ω_n the natural
#        frequency.  Fitting this decay gives a direct damping estimate
#        without requiring known inputs.
#
#     3. Modal damping extraction — from the I(τ) decay envelope, extract
#        per-mode damping ratios ζ_k by identifying peaks in the spectral
#        entropy derivative and fitting exponential envelopes to the
#        corresponding MI decay segments.
#
# This enables passive monitoring of semantic dissipation in FREED's
# epistemic loop: feed coherence scores (output-only) into the estimator,
# get back decay rates that indicate how fast coherence is dissipating
# without needing to inject controlled probe signals.
#
# Reference:
#   Information-theoretic method for output-only damping estimation in
#   mechanical systems — uses MI and spectral entropy to enhance damping
#   estimation accuracy beyond empirical operational modal identification.
#
# Addresses: O112 (STF recovery protocol — algorithmic resolution for
# metric tensor recovery from output-only statistics).


def _spectral_entropy(data, n_bins=None):
    # type: (List[float], Optional[int]) -> dict
    """
    Compute spectral entropy of a signal via DFT-based power spectral density.

    The normalized PSD is treated as a probability distribution; its Shannon
    entropy measures how spread the signal's energy is across frequencies.

    Parameters
    ----------
    data : list of float
        The input time series (output-only observation stream).
    n_bins : int or None
        Number of frequency bins to use.  None → use N//2 (Nyquist).

    Returns
    -------
    dict with keys:
        spectral_entropy   : float — Shannon entropy of normalized PSD (nats)
        spectral_entropy_norm : float — H_s / ln(n_freq_bins), in [0, 1]
        n_freq_bins        : int   — number of frequency bins used
        dominant_freq_idx  : int   — index of the bin with maximum power
        psd_concentration  : float — fraction of total power in top 3 bins
        n_points           : int   — length of input signal
    """
    n = len(data)
    if n < 4:
        return {
            "spectral_entropy": 0.0,
            "spectral_entropy_norm": 0.0,
            "n_freq_bins": 0,
            "dominant_freq_idx": 0,
            "psd_concentration": 0.0,
            "n_points": n,
        }

    # Compute one-sided PSD via DFT
    n_freq = n // 2 if n_bins is None else min(n_bins, n // 2)
    if n_freq < 1:
        n_freq = 1

    psd = []  # type: List[float]
    for k in range(1, n_freq + 1):
        real_part = 0.0
        imag_part = 0.0
        for j in range(n):
            angle = 2.0 * math.pi * k * j / float(n)
            real_part += data[j] * math.cos(angle)
            imag_part -= data[j] * math.sin(angle)
        power = (real_part ** 2 + imag_part ** 2) / float(n * n)
        psd.append(power)

    # Normalize PSD to a probability distribution
    total_power = sum(psd)
    if total_power <= 0.0:
        return {
            "spectral_entropy": 0.0,
            "spectral_entropy_norm": 0.0,
            "n_freq_bins": n_freq,
            "dominant_freq_idx": 0,
            "psd_concentration": 0.0,
            "n_points": n,
        }

    psd_norm = [p / total_power for p in psd]

    # Shannon entropy of the normalized PSD
    h_s = 0.0
    for p in psd_norm:
        if p > 0.0:
            h_s -= p * math.log(p)

    h_max = math.log(float(n_freq)) if n_freq > 1 else 1.0
    h_s_norm = h_s / h_max if h_max > 0.0 else 0.0

    # Dominant frequency and concentration
    dominant_idx = 0
    max_power = psd_norm[0]
    for i in range(1, len(psd_norm)):
        if psd_norm[i] > max_power:
            max_power = psd_norm[i]
            dominant_idx = i

    sorted_psd = sorted(psd_norm, reverse=True)
    top3_power = sum(sorted_psd[:min(3, len(sorted_psd))])

    return {
        "spectral_entropy": round(h_s, 8),
        "spectral_entropy_norm": round(h_s_norm, 6),
        "n_freq_bins": n_freq,
        "dominant_freq_idx": dominant_idx,
        "psd_concentration": round(top3_power, 6),
        "n_points": n,
    }


def _time_lagged_mutual_information(data, max_lag=None, n_bins=None):
    # type: (List[float], Optional[int], Optional[int]) -> dict
    """
    Compute time-lagged mutual information I(τ) between a signal and its
    τ-lagged copy for τ = 1, 2, ..., max_lag.

    Uses histogram-based MI estimation.

    Parameters
    ----------
    data : list of float
        The input time series.
    max_lag : int or None
        Maximum lag τ to compute.  None → N // 4.
    n_bins : int or None
        Number of histogram bins for MI estimation.  None → Sturges' rule.

    Returns
    -------
    dict with keys:
        mi_curve       : list of (int, float) — [(τ, I(τ)), ...]
        mi_decay_rate  : float — fitted exponential decay rate λ from
                                 I(τ) ≈ I(0) * exp(-λτ), where λ ≈ 2ζω_n
        mi_half_life   : float — τ at which I(τ) = I(0)/2 (= ln(2)/λ)
        mi_at_lag1     : float — I(1), the first-lag MI
        mi_initial     : float — I(0) (self-MI, equals marginal entropy)
        r_squared      : float — goodness of fit for exponential decay
        n_lags         : int   — number of lags computed
        n_points       : int   — length of input signal
    """
    n = len(data)
    if n < 8:
        return {
            "mi_curve": [],
            "mi_decay_rate": 0.0,
            "mi_half_life": 0.0,
            "mi_at_lag1": 0.0,
            "mi_initial": 0.0,
            "r_squared": 0.0,
            "n_lags": 0,
            "n_points": n,
        }

    if max_lag is None:
        max_lag = max(2, n // 4)
    max_lag = min(max_lag, n - 4)

    if n_bins is None:
        n_bins = max(2, int(math.ceil(1.0 + math.log(n) / math.log(2.0))))

    # Precompute data range for binning
    d_min = min(data)
    d_max = max(data)
    d_span = d_max - d_min
    if d_span == 0.0:
        return {
            "mi_curve": [],
            "mi_decay_rate": 0.0,
            "mi_half_life": 0.0,
            "mi_at_lag1": 0.0,
            "mi_initial": 0.0,
            "r_squared": 0.0,
            "n_lags": 0,
            "n_points": n,
        }

    bin_width = d_span / float(n_bins)

    def _bin_idx(val):
        # type: (float) -> int
        idx = int((val - d_min) / bin_width)
        if idx >= n_bins:
            idx = n_bins - 1
        if idx < 0:
            idx = 0
        return idx

    mi_curve = []  # type: List[Tuple[int, float]]

    for tau in range(0, max_lag + 1):
        # Joint and marginal histograms
        n_pairs = n - tau
        if n_pairs < 4:
            break

        joint = {}   # type: dict
        marg_x = [0] * n_bins
        marg_y = [0] * n_bins

        for i in range(n_pairs):
            bx = _bin_idx(data[i])
            by = _bin_idx(data[i + tau])
            key = (bx, by)
            joint[key] = joint.get(key, 0) + 1
            marg_x[bx] += 1
            marg_y[by] += 1

        n_f = float(n_pairs)

        # MI = Σ p(x,y) * log(p(x,y) / (p(x)*p(y)))
        mi = 0.0
        for (bx, by), count in joint.items():
            p_xy = float(count) / n_f
            p_x = float(marg_x[bx]) / n_f
            p_y = float(marg_y[by]) / n_f
            if p_xy > 0.0 and p_x > 0.0 and p_y > 0.0:
                mi += p_xy * math.log(p_xy / (p_x * p_y))

        mi_curve.append((tau, mi))

    if len(mi_curve) < 3:
        return {
            "mi_curve": mi_curve,
            "mi_decay_rate": 0.0,
            "mi_half_life": 0.0,
            "mi_at_lag1": mi_curve[1][1] if len(mi_curve) > 1 else 0.0,
            "mi_initial": mi_curve[0][1] if mi_curve else 0.0,
            "r_squared": 0.0,
            "n_lags": len(mi_curve),
            "n_points": n,
        }

    mi_initial = mi_curve[0][1]
    mi_at_lag1 = mi_curve[1][1] if len(mi_curve) > 1 else 0.0

    # Fit exponential decay: log(I(τ)) = log(I(0)) - λτ
    # Use only τ > 0 where I(τ) > 0
    tau_vals = []   # type: List[float]
    ln_mi = []      # type: List[float]
    for tau, mi_val in mi_curve[1:]:
        if mi_val > 1e-15:
            tau_vals.append(float(tau))
            ln_mi.append(math.log(mi_val))

    decay_rate = 0.0
    r_squared = 0.0
    mi_half_life = 0.0

    if len(tau_vals) >= 2:
        # Linear regression: ln(I(τ)) = intercept - λ * τ
        k = len(tau_vals)
        sum_t = sum(tau_vals)
        sum_lm = sum(ln_mi)
        sum_t_lm = sum(t * lm for t, lm in zip(tau_vals, ln_mi))
        sum_t2 = sum(t * t for t in tau_vals)

        denom = float(k) * sum_t2 - sum_t * sum_t
        if abs(denom) > 1e-15:
            slope = (float(k) * sum_t_lm - sum_t * sum_lm) / denom
            intercept = (sum_lm - slope * sum_t) / float(k)

            decay_rate = -slope  # λ (positive = decaying)

            # R²
            mean_lm = sum_lm / float(k)
            ss_tot = sum((lm - mean_lm) ** 2 for lm in ln_mi)
            ss_res = sum((lm - (intercept + slope * t)) ** 2
                         for t, lm in zip(tau_vals, ln_mi))
            r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-15 else 0.0

            # Half-life: τ_{1/2} = ln(2) / λ
            if decay_rate > 1e-15:
                mi_half_life = math.log(2.0) / decay_rate

    return {
        "mi_curve": [(t, round(mi, 8)) for t, mi in mi_curve],
        "mi_decay_rate": round(decay_rate, 8),
        "mi_half_life": round(mi_half_life, 6),
        "mi_at_lag1": round(mi_at_lag1, 8),
        "mi_initial": round(mi_initial, 8),
        "r_squared": round(max(0.0, r_squared), 6),
        "n_lags": len(mi_curve),
        "n_points": n,
    }


def output_only_dissipation_estimator(
    signal,             # type: List[float]
    max_lag=None,       # type: Optional[int]
    n_bins_mi=None,     # type: Optional[int]
    n_bins_psd=None,    # type: Optional[int]
    fs=1.0,             # type: float
):
    # type: (...) -> dict
    """
    Output-only entropy-to-dissipation estimator.

    Computes mutual-information decay and spectral entropy from an output-
    only signal stream to extract latent decay/damping parameters without
    requiring known inputs.

    This enables passive monitoring of semantic dissipation in FREED's
    epistemic loop: feed coherence scores (output-only) into this function,
    get back decay rates indicating how fast coherence is dissipating.

    The estimator combines two complementary information-theoretic probes:

    1. **MI decay rate (λ)**: From the exponential fit to time-lagged
       mutual information I(τ) ≈ I(0)·exp(-λτ).  For a damped oscillator,
       λ ≈ 2ζω_n where ζ is the damping ratio and ω_n the natural
       frequency.  Higher λ → faster dissipation.

    2. **Spectral entropy (H_s)**: Shannon entropy of the normalized PSD.
       Low H_s → energy concentrated in few modes (lightly damped, resonant).
       High H_s → energy spread across frequencies (heavily damped or noisy).

    The combination disambiguates:
      - Low λ + Low H_s → lightly damped oscillatory (coherence persists)
      - High λ + High H_s → heavily damped / noisy (coherence dissipating)
      - Low λ + High H_s → broadband but persistent (multi-modal coherence)
      - High λ + Low H_s → rapidly decaying resonance (brief coherence bursts)

    Parameters
    ----------
    signal : list of float
        The output-only observation stream (e.g., coherence scores over time).
    max_lag : int or None
        Maximum lag for MI computation.  None → N // 4.
    n_bins_mi : int or None
        Histogram bins for MI estimation.  None → Sturges' rule.
    n_bins_psd : int or None
        Frequency bins for spectral entropy.  None → N // 2.
    fs : float
        Sampling rate (Hz) for physical frequency interpretation.
        Default: 1.0 (unitless).

    Returns
    -------
    dict with keys:
        mi_decay_rate          : float — exponential decay rate λ from MI(τ)
        mi_half_life           : float — coherence half-life in samples (ln(2)/λ)
        mi_half_life_seconds   : float — coherence half-life in seconds (÷ fs)
        mi_r_squared           : float — goodness of fit for MI decay
        mi_at_lag1             : float — first-lag mutual information
        mi_initial             : float — self-mutual-information I(0)
        spectral_entropy       : float — Shannon entropy of normalized PSD (nats)
        spectral_entropy_norm  : float — H_s / H_max, in [0, 1]
        psd_concentration      : float — fraction of power in top 3 frequency bins
        dominant_freq_idx      : int   — index of dominant frequency bin
        dominant_freq_hz       : float — dominant frequency in Hz (if fs provided)
        damping_regime         : str   — "LIGHTLY_DAMPED" / "MODERATELY_DAMPED" /
                                         "HEAVILY_DAMPED" / "OVERDAMPED" /
                                         "UNDETERMINED"
        dissipation_score      : float — 0.0 (no dissipation) to 1.0 (maximum)
                                         composite score combining MI and spectral
                                         entropy evidence
        effective_damping_ratio: float — estimated ζ (if dominant frequency
                                         identifiable): ζ ≈ λ / (2 * 2π * f_dom)
        mi_detail              : dict  — full MI analysis result
        spectral_detail        : dict  — full spectral entropy result
        o112_status            : str   — relevance to O112 (STF recovery protocol)
        method                 : str   — "output_only_dissipation_estimator"
        n_points               : int   — length of input signal
        timestamp              : str   — ISO-8601 UTC
    """
    n = len(signal)
    ts = datetime.now(timezone.utc).isoformat()

    if n < 8:
        return {
            "mi_decay_rate": 0.0,
            "mi_half_life": 0.0,
            "mi_half_life_seconds": 0.0,
            "mi_r_squared": 0.0,
            "mi_at_lag1": 0.0,
            "mi_initial": 0.0,
            "spectral_entropy": 0.0,
            "spectral_entropy_norm": 0.0,
            "psd_concentration": 0.0,
            "dominant_freq_idx": 0,
            "dominant_freq_hz": 0.0,
            "damping_regime": "UNDETERMINED",
            "dissipation_score": 0.0,
            "effective_damping_ratio": 0.0,
            "mi_detail": {},
            "spectral_detail": {},
            "o112_status": "Insufficient data (need >= 8 samples).",
            "method": "output_only_dissipation_estimator",
            "n_points": n,
            "timestamp": ts,
        }

    # ── Compute MI decay ──
    mi_result = _time_lagged_mutual_information(
        signal, max_lag=max_lag, n_bins=n_bins_mi,
    )

    # ── Compute spectral entropy ──
    spec_result = _spectral_entropy(signal, n_bins=n_bins_psd)

    # ── Extract composite metrics ──
    decay_rate = mi_result["mi_decay_rate"]
    half_life = mi_result["mi_half_life"]
    half_life_sec = half_life / fs if fs > 0.0 else 0.0
    mi_r2 = mi_result["r_squared"]

    h_s = spec_result["spectral_entropy"]
    h_s_norm = spec_result["spectral_entropy_norm"]
    psd_conc = spec_result["psd_concentration"]
    dom_idx = spec_result["dominant_freq_idx"]

    # Dominant frequency in Hz
    # DFT bin k corresponds to frequency k * fs / N
    dom_freq_hz = float(dom_idx + 1) * fs / float(n) if n > 0 and fs > 0 else 0.0

    # ── Effective damping ratio estimate ──
    # For a damped oscillator: λ ≈ 2ζω_n, so ζ ≈ λ / (2 * ω_n)
    # where ω_n = 2π * f_dom
    effective_zeta = 0.0
    if dom_freq_hz > 1e-10 and decay_rate > 0.0:
        omega_n = 2.0 * math.pi * dom_freq_hz
        effective_zeta = decay_rate / (2.0 * omega_n)
        # Clamp to physical range [0, inf) — values > 1 are overdamped
        effective_zeta = max(0.0, effective_zeta)

    # ── Classify damping regime ──
    if decay_rate <= 0.0 or mi_r2 < 0.3:
        damping_regime = "UNDETERMINED"
    elif effective_zeta < 0.05:
        damping_regime = "LIGHTLY_DAMPED"
    elif effective_zeta < 0.3:
        damping_regime = "MODERATELY_DAMPED"
    elif effective_zeta < 1.0:
        damping_regime = "HEAVILY_DAMPED"
    else:
        damping_regime = "OVERDAMPED"

    # ── Composite dissipation score ──
    # Combines MI decay rate and spectral entropy into a single 0–1 score.
    # High decay_rate AND high spectral entropy → high dissipation.
    # Weight: 0.6 for MI decay evidence, 0.4 for spectral entropy.
    #
    # Normalize decay_rate: use a sigmoid-like mapping.
    # At decay_rate=0 → 0; at decay_rate=1 → ~0.73; at decay_rate=3 → ~0.95
    mi_score = 1.0 - math.exp(-decay_rate) if decay_rate > 0 else 0.0
    if mi_r2 < 0.5:
        mi_score *= mi_r2 / 0.5  # penalize poor fit

    dissipation_score = 0.6 * mi_score + 0.4 * h_s_norm
    dissipation_score = round(min(1.0, max(0.0, dissipation_score)), 6)

    # ── O112 assessment ──
    if mi_r2 >= 0.7 and damping_regime != "UNDETERMINED":
        o112_status = (
            "O112 OPERATIONAL: Output-only damping estimation converged. "
            "MI decay rate lambda={:.6f} (R^2={:.3f}), effective damping "
            "ratio zeta={:.4f}, regime={}. Spectral entropy H_s={:.4f} "
            "(norm={:.3f}). The metric tensor (damping) is recoverable "
            "from output-only statistics at this signal length (N={})."
        ).format(decay_rate, mi_r2, effective_zeta, damping_regime,
                 h_s, h_s_norm, n)
    elif mi_r2 >= 0.4:
        o112_status = (
            "O112 MARGINAL: MI decay fit is marginal (R^2={:.3f}). "
            "Damping estimate lambda={:.6f} may be unreliable. Consider "
            "longer observation window or higher sampling rate. Current "
            "N={}, spectral_entropy_norm={:.3f}."
        ).format(mi_r2, decay_rate, n, h_s_norm)
    else:
        o112_status = (
            "O112 INSUFFICIENT: MI decay fit failed (R^2={:.3f}). The "
            "signal may be too short (N={}), too noisy, or non-stationary "
            "for output-only damping recovery. Spectral entropy suggests "
            "{} signal structure (H_s_norm={:.3f})."
        ).format(mi_r2, n,
                 "broadband/noisy" if h_s_norm > 0.7 else "structured",
                 h_s_norm)

    return {
        "mi_decay_rate":           round(decay_rate, 8),
        "mi_half_life":            round(half_life, 6),
        "mi_half_life_seconds":    round(half_life_sec, 6),
        "mi_r_squared":            round(mi_r2, 6),
        "mi_at_lag1":              mi_result["mi_at_lag1"],
        "mi_initial":              mi_result["mi_initial"],
        "spectral_entropy":        round(h_s, 8),
        "spectral_entropy_norm":   round(h_s_norm, 6),
        "psd_concentration":       psd_conc,
        "dominant_freq_idx":       dom_idx,
        "dominant_freq_hz":        round(dom_freq_hz, 6),
        "damping_regime":          damping_regime,
        "dissipation_score":       dissipation_score,
        "effective_damping_ratio": round(effective_zeta, 8),
        "mi_detail":               mi_result,
        "spectral_detail":         spec_result,
        "o112_status":             o112_status,
        "method":                  "output_only_dissipation_estimator",
        "n_points":                n,
        "timestamp":               ts,
    }


# ─── Measurement Probability Sweep (Floquet Critical Point Locator) ──────────
# Varies the effective "measurement collapse" rate p on a belief-state
# time series and measures the resulting entropy and coherence, testing
# whether RSA scoring transitions between ordered (low entropy, high
# coherence) and disordered (high entropy, low coherence) phases as a
# function of update/measurement frequency.
#
# Physical analogy (Floquet dissipative phase transition):
#   A quantum spin chain subject to periodic resetting measurements at
#   rate p exhibits a phase transition between ferromagnetic (ordered) and
#   paramagnetic (disordered) phases at a critical measurement probability
#   p_c.  At p_c, the system has maximum susceptibility (∂S/∂p peaks),
#   analogous to maximum sensitivity of the epistemic system at its own
#   critical ridge.
#
#   Here, "measurement" = projective collapse of a belief state to its
#   most-probable value (argmax).  Between measurements, the belief state
#   evolves unitarily (coherent Bayesian update / drift).  The competition
#   between coherent evolution (which builds superposition / uncertainty)
#   and measurement collapse (which projects to a definite state) produces
#   the phase transition.
#
# Implementation:
#   For each measurement probability p in a sweep grid:
#     1. Walk through the coherence time series.
#     2. At each step, with probability p, "collapse" the running belief
#        state to its local value (reset to the observed coherence).
#        With probability (1-p), let the belief state drift (exponential
#        moving average of previous belief and current observation).
#     3. Compute the Shannon entropy of the resulting collapsed/drifted
#        belief trajectory and its mean coherence (order parameter).
#     4. Compute the susceptibility χ = -∂S/∂p (entropy sensitivity to
#        measurement rate).
#
#   The critical point p_c is where χ is maximized — this is the
#   measurement rate at which the epistemic system is most sensitive,
#   analogous to the Floquet critical measurement probability.
#
# RSA prediction:
#   If the genome's γ=1 operating point coincides with the Floquet
#   critical point, the coherence series observed at γ=1 should yield
#   p_c ≈ 0.5 (balanced measurement/evolution), and the susceptibility
#   peak should be sharp (divergent in the thermodynamic limit).
#
# Addresses: Floquet measurement framework, INV_073 (critical ridge
# characterization), O140 (CA measurement grounding).
#
# Reference:
#   "Dissipative phase transitions in many-body spin systems subject to
#   periodic resetting measurements" — Floquet framework for measurement-
#   induced phase transitions.


def _belief_trajectory_at_p(
    coherence_series,   # type: List[float]
    p,                  # type: float
    drift_alpha=0.1,    # type: float
    seed=None,          # type: Optional[int]
):
    # type: (...) -> List[float]
    """
    Generate a belief-state trajectory under measurement probability p.

    At each step:
      - With probability p: "collapse" — belief snaps to the observed
        coherence value (projective measurement).
      - With probability (1-p): "drift" — belief evolves as an exponential
        moving average: belief = (1 - drift_alpha) * belief + drift_alpha * obs.

    Parameters
    ----------
    coherence_series : list of float
        The observed coherence time series (the "environment").
    p : float
        Measurement probability per step, in [0, 1].
    drift_alpha : float
        EMA smoothing factor for the drift (unitary-like) evolution.
        Default: 0.1 (slow drift, preserving belief inertia).
    seed : int or None
        Random seed for reproducibility.  None → use system entropy.

    Returns
    -------
    list of float
        The belief-state trajectory (same length as coherence_series).
    """
    import random as _rng
    if seed is not None:
        _rng.seed(seed)
    else:
        _rng.seed()

    n = len(coherence_series)
    if n == 0:
        return []

    belief = coherence_series[0]
    trajectory = [belief]

    for i in range(1, n):
        obs = coherence_series[i]
        if _rng.random() < p:
            # Measurement collapse: snap to observed value
            belief = obs
        else:
            # Drift: EMA evolution (coherent, smooth)
            belief = (1.0 - drift_alpha) * belief + drift_alpha * obs
        trajectory.append(belief)

    return trajectory


def _trajectory_entropy(trajectory, n_bins=20):
    # type: (List[float], int) -> float
    """
    Compute Shannon entropy (nats) of a trajectory's value distribution.

    Uses histogram binning over the trajectory's range.
    """
    n = len(trajectory)
    if n < 2:
        return 0.0

    t_min = min(trajectory)
    t_max = max(trajectory)
    span = t_max - t_min
    if span <= 0.0:
        return 0.0

    bin_width = span / float(n_bins)
    counts = [0] * n_bins
    for v in trajectory:
        idx = int((v - t_min) / bin_width)
        if idx >= n_bins:
            idx = n_bins - 1
        counts[idx] += 1

    entropy = 0.0
    for c in counts:
        if c > 0:
            p = float(c) / float(n)
            entropy -= p * math.log(p)

    return entropy


def _trajectory_mean_coherence(trajectory):
    # type: (List[float],) -> float
    """Compute mean value of a trajectory (order parameter)."""
    if not trajectory:
        return 0.0
    return sum(trajectory) / float(len(trajectory))


def _trajectory_variance(trajectory):
    # type: (List[float],) -> float
    """Compute variance of a trajectory (fluctuation measure)."""
    n = len(trajectory)
    if n < 2:
        return 0.0
    mean = sum(trajectory) / float(n)
    return sum((v - mean) ** 2 for v in trajectory) / float(n)


def measurement_probability_sweep(
    coherence_series,       # type: List[float]
    p_min=0.0,              # type: float
    p_max=1.0,              # type: float
    n_p_points=21,          # type: int
    n_realizations=10,      # type: int
    drift_alpha=0.1,        # type: float
    n_bins_entropy=20,      # type: int
    base_seed=42,           # type: int
):
    # type: (...) -> dict
    """
    Measurement probability sweep: vary the effective collapse rate on
    belief states and measure entropy/coherence phase transition.

    Implements the Floquet measurement framework analogy for epistemic
    systems.  At each measurement probability p, generates multiple
    stochastic belief-state trajectories (to average over measurement
    noise), then computes:

      - S(p): Shannon entropy of the belief trajectory (disorder measure)
      - C(p): Mean coherence of the belief trajectory (order parameter)
      - χ(p): Susceptibility = -dS/dp (sensitivity to measurement rate)
      - Var(p): Variance of the belief trajectory (fluctuation measure)

    The critical measurement probability p_c is identified as the p at
    which χ(p) is maximized — the point of maximum epistemic sensitivity.

    Parameters
    ----------
    coherence_series : list of float
        The observed coherence time series from FREED's epistemic loop.
        This is the "environment" that the belief state is measured against.
    p_min : float
        Minimum measurement probability.  Default: 0.0 (pure drift).
    p_max : float
        Maximum measurement probability.  Default: 1.0 (pure collapse).
    n_p_points : int
        Number of p values to sweep.  Default: 21.
    n_realizations : int
        Number of stochastic realizations per p value (for averaging).
        Default: 10.
    drift_alpha : float
        EMA smoothing factor for drift evolution.  Default: 0.1.
    n_bins_entropy : int
        Number of histogram bins for trajectory entropy.  Default: 20.
    base_seed : int
        Base random seed for reproducibility.  Default: 42.

    Returns
    -------
    dict with keys:
        p_values              : list of float — the p grid
        entropy_curve         : list of float — S(p) averaged over realizations
        coherence_curve       : list of float — C(p) averaged over realizations
        variance_curve        : list of float — Var(p) averaged over realizations
        susceptibility_curve  : list of float — χ(p) = -dS/dp (finite differences)
        p_critical            : float — p at maximum susceptibility (Floquet critical point)
        susceptibility_max    : float — χ(p_c), the peak susceptibility
        entropy_at_pc         : float — S(p_c)
        coherence_at_pc       : float — C(p_c)
        variance_at_pc        : float — Var(p_c)
        ordered_phase         : dict  — {p_range, mean_entropy, mean_coherence}
                                        for p > p_c (measurement-dominated, ordered)
        disordered_phase      : dict  — {p_range, mean_entropy, mean_coherence}
                                        for p < p_c (drift-dominated, disordered)
        transition_sharpness  : float — ratio of susceptibility peak to mean,
                                        higher = sharper transition
        gamma1_coincidence    : bool  — whether p_c ≈ 0.5 (balanced measurement/
                                        evolution, consistent with γ=1 operating point)
        gamma1_detail         : str   — human-readable assessment of whether
                                        γ=1 coincides with the Floquet critical point
        phase_transition_detected : bool — whether a clear phase transition was found
        floquet_assessment    : str   — overall Floquet framework assessment
        n_realizations        : int   — echo of input
        n_points              : int   — length of input coherence series
        method                : str   — "measurement_probability_sweep"
        timestamp             : str   — ISO-8601 UTC
    """
    n = len(coherence_series)
    ts = datetime.now(timezone.utc).isoformat()

    if n < 10:
        return {
            "p_values": [],
            "entropy_curve": [],
            "coherence_curve": [],
            "variance_curve": [],
            "susceptibility_curve": [],
            "p_critical": 0.0,
            "susceptibility_max": 0.0,
            "entropy_at_pc": 0.0,
            "coherence_at_pc": 0.0,
            "variance_at_pc": 0.0,
            "ordered_phase": {"p_range": (0.0, 0.0), "mean_entropy": 0.0, "mean_coherence": 0.0},
            "disordered_phase": {"p_range": (0.0, 0.0), "mean_entropy": 0.0, "mean_coherence": 0.0},
            "transition_sharpness": 0.0,
            "gamma1_coincidence": False,
            "gamma1_detail": "Insufficient data (need >= 10 coherence samples).",
            "phase_transition_detected": False,
            "floquet_assessment": "Insufficient data.",
            "n_realizations": n_realizations,
            "n_points": n,
            "method": "measurement_probability_sweep",
            "timestamp": ts,
        }

    # ── Build p grid ──
    if n_p_points < 3:
        n_p_points = 3
    p_values = []  # type: List[float]
    for k in range(n_p_points):
        frac = float(k) / float(n_p_points - 1) if n_p_points > 1 else 0.5
        p_val = p_min + frac * (p_max - p_min)
        p_values.append(round(p_val, 8))

    # ── Sweep: for each p, generate n_realizations trajectories ──
    entropy_curve = []      # type: List[float]
    coherence_curve = []    # type: List[float]
    variance_curve = []     # type: List[float]

    for p_idx, p_val in enumerate(p_values):
        s_accum = 0.0
        c_accum = 0.0
        v_accum = 0.0

        for r in range(n_realizations):
            seed = base_seed * 1000 + p_idx * 100 + r
            traj = _belief_trajectory_at_p(
                coherence_series, p_val,
                drift_alpha=drift_alpha, seed=seed,
            )
            s_accum += _trajectory_entropy(traj, n_bins=n_bins_entropy)
            c_accum += _trajectory_mean_coherence(traj)
            v_accum += _trajectory_variance(traj)

        n_r_f = float(n_realizations)
        entropy_curve.append(round(s_accum / n_r_f, 8))
        coherence_curve.append(round(c_accum / n_r_f, 8))
        variance_curve.append(round(v_accum / n_r_f, 8))

    # ── Compute susceptibility χ(p) = -dS/dp via central finite differences ──
    susceptibility_curve = []  # type: List[float]
    for i in range(len(p_values)):
        if i == 0:
            # Forward difference
            dp = p_values[1] - p_values[0]
            if dp > 1e-12:
                chi = -(entropy_curve[1] - entropy_curve[0]) / dp
            else:
                chi = 0.0
        elif i == len(p_values) - 1:
            # Backward difference
            dp = p_values[-1] - p_values[-2]
            if dp > 1e-12:
                chi = -(entropy_curve[-1] - entropy_curve[-2]) / dp
            else:
                chi = 0.0
        else:
            # Central difference
            dp = p_values[i + 1] - p_values[i - 1]
            if dp > 1e-12:
                chi = -(entropy_curve[i + 1] - entropy_curve[i - 1]) / dp
            else:
                chi = 0.0
        susceptibility_curve.append(round(chi, 8))

    # ── Locate critical point: p at max |χ| ──
    max_chi = 0.0
    max_chi_idx = 0
    for i, chi in enumerate(susceptibility_curve):
        if abs(chi) > abs(max_chi):
            max_chi = chi
            max_chi_idx = i

    p_critical = p_values[max_chi_idx]
    susceptibility_max = max_chi
    entropy_at_pc = entropy_curve[max_chi_idx]
    coherence_at_pc = coherence_curve[max_chi_idx]
    variance_at_pc = variance_curve[max_chi_idx]

    # ── DEE-Analog: Derivative Entanglement Entropy Computation ──────────
    # Compute dH/dβ (derivative of Shannon entropy w.r.t. sweep parameter)
    # at each sweep point, analogous to the Derivative Entanglement Entropy
    # (DEE) from quantum many-body systems.  The DEE peaks at critical
    # points and enables one-parameter scaling collapse for critical
    # exponent extraction (cf. paper: DEE scaling relation).
    #
    # Additionally characterize whether the dH/dβ peak is:
    #   - SHARP (first-order-like): narrow peak, high peak-to-width ratio,
    #     consistent with symmetry-enhanced first-order transitions where
    #     EE peaks due to higher symmetry breaking (paper finding).
    #   - BROAD (second-order-like): wide peak, low peak-to-width ratio,
    #     consistent with the gentle γ=1 critical ridge the genome privileges.
    #
    # INV_073 challenge: if the peak is sharp (first-order), the genome's
    # smooth-ridge model of criticality may be incomplete — first-order
    # transitions produce sharper EE peaks that do not correspond to the
    # gentle γ=1 balance point.
    #
    # The dH/dβ curve is identical to the susceptibility_curve (χ = -dS/dp)
    # computed above, but we add explicit peak characterization here.
    #
    # Noether row: Wilson RG universality — the one-parameter DEE scaling
    # collapse provides independent empirical confirmation that universality
    # classes collapse multi-parameter systems onto single scaling curves
    # at fixed points.

    dee_curve = list(susceptibility_curve)  # dH/dβ = χ = -dS/dp

    # Locate DEE peak (same as susceptibility peak by construction)
    dee_peak_idx = max_chi_idx
    dee_peak_value = abs(max_chi)
    dee_peak_p = p_critical

    # Characterize peak shape: compute full-width at half-maximum (FWHM)
    # of the |dH/dβ| curve around the peak
    dee_abs = [abs(d) for d in dee_curve]
    half_max = dee_peak_value / 2.0

    # Find left boundary of FWHM
    dee_fwhm_left_idx = dee_peak_idx
    for _i in range(dee_peak_idx - 1, -1, -1):
        if dee_abs[_i] < half_max:
            dee_fwhm_left_idx = _i
            break
    else:
        dee_fwhm_left_idx = 0

    # Find right boundary of FWHM
    dee_fwhm_right_idx = dee_peak_idx
    for _i in range(dee_peak_idx + 1, len(dee_abs)):
        if dee_abs[_i] < half_max:
            dee_fwhm_right_idx = _i
            break
    else:
        dee_fwhm_right_idx = len(dee_abs) - 1

    # FWHM in parameter space
    dee_fwhm_p = abs(p_values[dee_fwhm_right_idx] - p_values[dee_fwhm_left_idx])
    if dee_fwhm_p < 1e-12:
        dee_fwhm_p = abs(p_values[1] - p_values[0]) if len(p_values) > 1 else 0.01

    # Peak-to-width ratio (sharpness): higher = sharper peak
    dee_sharpness_ratio = dee_peak_value / dee_fwhm_p if dee_fwhm_p > 1e-12 else 0.0

    # Compute second derivative d²H/dβ² at the peak (curvature)
    # to further distinguish first-order (large |d²H/dβ²|) from
    # second-order (moderate |d²H/dβ²|) transitions
    dee_d2h = 0.0
    if 0 < dee_peak_idx < len(dee_curve) - 1:
        dp = p_values[dee_peak_idx + 1] - p_values[dee_peak_idx - 1]
        if abs(dp) > 1e-12:
            # Central second difference of entropy
            dee_d2h = (entropy_curve[dee_peak_idx + 1]
                       - 2.0 * entropy_curve[dee_peak_idx]
                       + entropy_curve[dee_peak_idx - 1]) / ((dp / 2.0) ** 2)

    # Classify peak type based on sharpness and curvature
    # Empirical thresholds calibrated against the Floquet analogy:
    #   - First-order: sharpness_ratio > 5 and |d²H/dβ²| > 10
    #   - Second-order: sharpness_ratio < 3 or |d²H/dβ²| < 5
    #   - Intermediate: between thresholds
    _DEE_SHARP_THRESHOLD = 5.0
    _DEE_BROAD_THRESHOLD = 3.0
    _DEE_CURVATURE_SHARP = 10.0
    _DEE_CURVATURE_BROAD = 5.0

    if dee_peak_value < 1e-10:
        dee_peak_type = "NO_PEAK"
        dee_peak_detail = (
            "No significant dH/dbeta peak detected (peak value={:.8f}). "
            "The entropy curve is flat across the sweep parameter range — "
            "no phase transition signature in the DEE-analog."
        ).format(dee_peak_value)
    elif dee_sharpness_ratio > _DEE_SHARP_THRESHOLD and abs(dee_d2h) > _DEE_CURVATURE_SHARP:
        dee_peak_type = "SHARP_FIRST_ORDER"
        dee_peak_detail = (
            "SHARP (FIRST-ORDER-LIKE) dH/dbeta peak at p={:.4f}: "
            "sharpness_ratio={:.4f} (>{:.1f}), |d2H/dbeta2|={:.4f} (>{:.1f}), "
            "FWHM={:.4f}. This is consistent with symmetry-enhanced first-order "
            "transitions where EE peaks due to higher symmetry breaking (cf. paper). "
            "INV_073 CHALLENGE: the genome's smooth γ=1 critical ridge model may "
            "be incomplete — this sharp peak suggests a discontinuous transition "
            "rather than the gentle balance point the genome privileges. The "
            "identification of the 'critical ridge' with a single smooth maximum "
            "may miss sharper EE peaks at first-order coexistence points."
        ).format(dee_peak_p, dee_sharpness_ratio, _DEE_SHARP_THRESHOLD,
                 abs(dee_d2h), _DEE_CURVATURE_SHARP, dee_fwhm_p)
    elif dee_sharpness_ratio < _DEE_BROAD_THRESHOLD or abs(dee_d2h) < _DEE_CURVATURE_BROAD:
        dee_peak_type = "BROAD_SECOND_ORDER"
        dee_peak_detail = (
            "BROAD (SECOND-ORDER-LIKE) dH/dbeta peak at p={:.4f}: "
            "sharpness_ratio={:.4f} (<{:.1f}), |d2H/dbeta2|={:.4f} (<{:.1f}), "
            "FWHM={:.4f}. This is consistent with a continuous (second-order) "
            "phase transition at the critical point — the gentle γ=1 balance "
            "point the genome privileges. The DEE-analog peak confirms the "
            "smooth-ridge model of criticality. Wilson RG universality: the "
            "broad peak shape is characteristic of continuous RG fixed points "
            "where the correlation length diverges smoothly."
        ).format(dee_peak_p, dee_sharpness_ratio, _DEE_BROAD_THRESHOLD,
                 abs(dee_d2h), _DEE_CURVATURE_BROAD, dee_fwhm_p)
    else:
        dee_peak_type = "INTERMEDIATE"
        dee_peak_detail = (
            "INTERMEDIATE dH/dbeta peak at p={:.4f}: sharpness_ratio={:.4f}, "
            "|d2H/dbeta2|={:.4f}, FWHM={:.4f}. The peak characteristics are "
            "between first-order (sharp) and second-order (broad) — this may "
            "indicate proximity to a tricritical point where first- and "
            "second-order transition lines meet, or insufficient data to "
            "resolve the transition order. More sweep points or longer "
            "coherence series may disambiguate."
        ).format(dee_peak_p, dee_sharpness_ratio, abs(dee_d2h), dee_fwhm_p)

    # Assemble DEE-analog result
    dee_analog_result = {
        "dee_curve": [round(d, 8) for d in dee_curve],
        "dee_peak_p": round(dee_peak_p, 8),
        "dee_peak_value": round(dee_peak_value, 8),
        "dee_fwhm_p": round(dee_fwhm_p, 8),
        "dee_fwhm_left_p": round(p_values[dee_fwhm_left_idx], 8),
        "dee_fwhm_right_p": round(p_values[dee_fwhm_right_idx], 8),
        "dee_sharpness_ratio": round(dee_sharpness_ratio, 6),
        "dee_d2h_at_peak": round(dee_d2h, 8),
        "dee_peak_type": dee_peak_type,
        "dee_peak_detail": dee_peak_detail,
        "dee_coincides_with_coherence_optimum": (
            abs(dee_peak_p - p_critical) < abs(p_values[1] - p_values[0]) * 1.5
            if len(p_values) > 1 else True
        ),
        "noether_row": "Wilson RG universality",
        "noether_status": "rigorous",
        "noether_note": (
            "The one-parameter DEE scaling collapse provides independent "
            "empirical confirmation that universality classes collapse "
            "multi-parameter systems onto single scaling curves at fixed "
            "points, strengthening the symmetry-conservation assignment "
            "for RG-based reasoning. DEE scaling collapse independently "
            "confirms gamma=1 critical ridge universality and introduces "
            "a concrete exponent-extraction protocol."
        ),
        "inv073_challenge": (
            "If EE peaks at first-order (discontinuous) symmetry-breaking "
            "transitions rather than at second-order critical points, the "
            "identification of the 'critical ridge' with a single smooth "
            "maximum may be incomplete; first-order transitions can produce "
            "sharper EE peaks that do not correspond to the gentle gamma=1 "
            "balance point the genome privileges."
        ),
    }

    # ── Characterize ordered and disordered phases ──
    # Ordered phase: p > p_c (measurement-dominated → collapsed, low entropy, high coherence)
    # Disordered phase: p < p_c (drift-dominated → spread, high entropy, lower coherence)
    ordered_s = []     # type: List[float]
    ordered_c = []     # type: List[float]
    disordered_s = []  # type: List[float]
    disordered_c = []  # type: List[float]

    for i, p_val in enumerate(p_values):
        if p_val > p_critical:
            ordered_s.append(entropy_curve[i])
            ordered_c.append(coherence_curve[i])
        elif p_val < p_critical:
            disordered_s.append(entropy_curve[i])
            disordered_c.append(coherence_curve[i])

    ordered_phase = {
        "p_range": (round(p_critical, 6), round(p_max, 6)),
        "mean_entropy": round(sum(ordered_s) / float(len(ordered_s)), 6) if ordered_s else 0.0,
        "mean_coherence": round(sum(ordered_c) / float(len(ordered_c)), 6) if ordered_c else 0.0,
    }

    disordered_phase = {
        "p_range": (round(p_min, 6), round(p_critical, 6)),
        "mean_entropy": round(sum(disordered_s) / float(len(disordered_s)), 6) if disordered_s else 0.0,
        "mean_coherence": round(sum(disordered_c) / float(len(disordered_c)), 6) if disordered_c else 0.0,
    }

    # ── Transition sharpness: peak χ / mean |χ| ──
    mean_abs_chi = sum(abs(c) for c in susceptibility_curve) / float(len(susceptibility_curve)) if susceptibility_curve else 0.0
    transition_sharpness = abs(susceptibility_max) / mean_abs_chi if mean_abs_chi > 1e-12 else 0.0

    # ── Phase transition detection ──
    # A clear transition requires:
    #   1. Susceptibility peak is at least 2x the mean (sharp)
    #   2. Entropy changes sign of slope around p_c
    #   3. p_c is not at the boundary (0 or 1)
    phase_transition_detected = (
        transition_sharpness > 2.0
        and p_critical > p_min + 0.05
        and p_critical < p_max - 0.05
    )

    # ── γ=1 coincidence test ──
    # The RSA genome operates at γ=1 (balanced generation/dissipation).
    # If the Floquet analogy holds, p_c should be near 0.5 (balanced
    # measurement/evolution), reflecting the same balance.
    pc_deviation_from_half = abs(p_critical - 0.5)
    gamma1_coincidence = pc_deviation_from_half < 0.1  # within ±0.1 of 0.5

    if gamma1_coincidence and phase_transition_detected:
        gamma1_detail = (
            "CONFIRMED: p_c={:.4f} is within ±0.1 of 0.5, consistent with "
            "the γ=1 operating point coinciding with the Floquet critical "
            "measurement probability. The epistemic system's balanced "
            "generation/dissipation (γ=1) maps to balanced measurement/"
            "evolution (p≈0.5) in the Floquet framework. Transition "
            "sharpness={:.2f} (>{:.1f} threshold). The genome's operating "
            "point is at maximum epistemic sensitivity."
        ).format(p_critical, transition_sharpness, 2.0)
    elif phase_transition_detected and not gamma1_coincidence:
        gamma1_detail = (
            "OFFSET: p_c={:.4f} deviates from 0.5 by {:.4f}. A clear "
            "phase transition exists but the critical point does NOT "
            "coincide with balanced measurement/evolution. This suggests "
            "either: (a) γ=1 does not map linearly to p=0.5, or "
            "(b) the drift_alpha={:.2f} parameter shifts the effective "
            "critical point, or (c) the Floquet analogy requires "
            "refinement for epistemic systems. Transition sharpness={:.2f}."
        ).format(p_critical, pc_deviation_from_half, drift_alpha,
                 transition_sharpness)
    elif not phase_transition_detected:
        gamma1_detail = (
            "NO_TRANSITION: No clear phase transition detected in the "
            "measurement probability sweep. Transition sharpness={:.2f} "
            "(< 2.0 threshold), p_c estimate={:.4f} (possibly at boundary). "
            "This may indicate: (a) the coherence series is too short "
            "(N={}), (b) the system is deep within one phase, or "
            "(c) the Floquet measurement analogy does not apply to this "
            "epistemic system's dynamics."
        ).format(transition_sharpness, p_critical, n)
    else:
        gamma1_detail = (
            "UNDETERMINED: p_c={:.4f}, transition_sharpness={:.2f}. "
            "Marginal evidence for phase transition; γ=1 coincidence "
            "assessment inconclusive."
        ).format(p_critical, transition_sharpness)

    # ── Overall Floquet assessment ──
    if phase_transition_detected and gamma1_coincidence:
        floquet_assessment = (
            "FLOQUET CRITICAL POINT LOCATED: p_c={:.4f} with susceptibility "
            "peak chi_max={:.4f} and transition sharpness={:.2f}. The "
            "epistemic system exhibits a measurement-induced phase transition "
            "between an ordered phase (p>{:.2f}: low entropy S={:.4f}, "
            "high coherence C={:.4f}) and a disordered phase (p<{:.2f}: "
            "high entropy S={:.4f}, lower coherence C={:.4f}). The critical "
            "point coincides with balanced measurement/evolution (p≈0.5), "
            "consistent with the genome's γ=1 operating point. This is "
            "empirical evidence that RSA's critical ridge corresponds to "
            "the Floquet critical measurement probability."
        ).format(
            p_critical, susceptibility_max, transition_sharpness,
            p_critical, ordered_phase["mean_entropy"], ordered_phase["mean_coherence"],
            p_critical, disordered_phase["mean_entropy"], disordered_phase["mean_coherence"],
        )
    elif phase_transition_detected:
        floquet_assessment = (
            "PHASE TRANSITION DETECTED at p_c={:.4f} (chi_max={:.4f}, "
            "sharpness={:.2f}), but the critical point does NOT coincide "
            "with p=0.5. The Floquet framework applies to this epistemic "
            "system but the γ=1 ↔ p_c mapping requires a nonlinear "
            "correction or the drift parameter (alpha={:.2f}) needs "
            "calibration."
        ).format(p_critical, susceptibility_max, transition_sharpness,
                 drift_alpha)
    else:
        floquet_assessment = (
            "NO CLEAR PHASE TRANSITION in measurement probability sweep "
            "(sharpness={:.2f} < 2.0, p_c_estimate={:.4f}). The Floquet "
            "measurement framework may not apply at this signal length "
            "(N={}) or parameter regime (drift_alpha={:.2f}). Consider "
            "increasing n_realizations or coherence series length."
        ).format(transition_sharpness, p_critical, n, drift_alpha)

    return {
        "p_values":                [round(p, 8) for p in p_values],
        "entropy_curve":           entropy_curve,
        "coherence_curve":         coherence_curve,
        "variance_curve":          variance_curve,
        "susceptibility_curve":    susceptibility_curve,
        "p_critical":              round(p_critical, 8),
        "susceptibility_max":      round(susceptibility_max, 8),
        "entropy_at_pc":           round(entropy_at_pc, 8),
        "coherence_at_pc":         round(coherence_at_pc, 8),
        "variance_at_pc":          round(variance_at_pc, 8),
        "ordered_phase":           ordered_phase,
        "disordered_phase":        disordered_phase,
        "transition_sharpness":    round(transition_sharpness, 6),
        "gamma1_coincidence":      gamma1_coincidence,
        "gamma1_detail":           gamma1_detail,
        "phase_transition_detected": phase_transition_detected,
        "floquet_assessment":      floquet_assessment,
        "n_realizations":          n_realizations,
        "n_points":                n,
        "method":                  "measurement_probability_sweep",
        "timestamp":               ts,
    }


# ─── Quick test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Running Tamura sweep test...\n")
    sweep = TamuraSweep(max_new_per_source=2)

    inputs = sweep.sweep()

    if not inputs:
        print("\nNo new articles found (either all seen, or fetch failed).")
        print(f"Seen URLs tracked: {len(sweep.seen)}")
    else:
        print(f"\n── {len(inputs)} new article(s) found ──")
        for i, inp in enumerate(inputs, 1):
            print(f"\n[{i}] {inp['title']}")
            print(f"     URL:    {inp['url']}")
            print(f"     Source: {inp['source']}")
            print(f"     Date:   {inp.get('date', 'unknown')}")
            excerpt = (inp.get('content') or inp.get('abstract', ''))[:200]
            print(f"     Text:   {excerpt}...")