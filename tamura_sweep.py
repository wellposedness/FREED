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

    return {
        "delta": delta,
        "intercept": intercept,
        "r_squared": r_squared,
        "entropy_curve": entropy_curve,
        "saturation_idx": saturation_idx,
        "n_points": n,
        "t_range": (t_min, t_max),
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


def _criticality_verdict(sigma, alpha, r_squared):
    # type: (float, float, float) -> str
    """
    Classify criticality state from branching ratio and power-law exponent.

    Returns one of:
        AT_CRITICAL   — σ in critical band, power-law confirmed
        NEAR_CRITICAL — σ in band but power-law weak, or σ near band edge
        SUPERCRITICAL — σ > 1.05 (γ<1 dissipation risk)
        SUBCRITICAL   — σ < 0.95 (γ>1 freeze risk)
        UNDETERMINED  — insufficient data
    """
    if sigma == 0.0 and alpha == 0.0:
        return "UNDETERMINED"

    in_band = SIGMA_CRITICAL_LOW <= sigma <= SIGMA_CRITICAL_HIGH
    power_law_ok = (ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
                    and r_squared >= ALPHA_R2_THRESHOLD)

    if in_band and power_law_ok:
        return "AT_CRITICAL"
    elif in_band:
        return "NEAR_CRITICAL"
    elif sigma > SIGMA_CRITICAL_HIGH:
        return "SUPERCRITICAL"
    elif sigma < SIGMA_CRITICAL_LOW:
        return "SUBCRITICAL"
    else:
        return "UNDETERMINED"


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
):
    # type: (...) -> dict
    """
    Score a single CA generation's criticality telemetry.

    Produces a scored telemetry record with σ, α, Shannon entropy,
    survival rate, criticality verdict, and drift indicators for
    longitudinal tracking.

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
        timestamp        : str     — ISO-8601 UTC timestamp
    """
    verdict = _criticality_verdict(sigma, alpha, alpha_r_squared)
    sigma_drift = abs(sigma - 1.0)
    h_fraction = (shannon_h / shannon_h_max) if shannon_h_max > 0.0 else 0.0
    alpha_in_soc = ALPHA_SOC_LOW <= alpha <= ALPHA_SOC_HIGH
    power_law_likely = alpha_r_squared >= ALPHA_R2_THRESHOLD

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

        return {
            "sigma_mean": round(s_mean, 6),
            "sigma_std": round(s_std, 6),
            "alpha": round(alpha, 4),
            "alpha_r_squared": round(r_squared, 4),
            "power_law_likely": power_law_likely,
            "in_critical_band": in_band,
            "drift_event": None,
            "n_steps": n_steps,
            "n_avalanches": len(self._avalanche_sizes),
            "verdict": verdict,
        }

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
        """
        self._load_seen()   # reload from disk — targeted_sweep may have written new entries
        all_inputs = []

        for source in SOURCES:
            print(f"[SWEEP] Fetching: {source['name']}")
            try:
                inputs = self._sweep_source(source)
                all_inputs.extend(inputs)
                if inputs:
                    print(f"[SWEEP]   → {len(inputs)} new article(s).")
                else:
                    print(f"[SWEEP]   → No new articles.")
            except Exception as e:
                print(f"[SWEEP]   → Error: {e}")

            time.sleep(POLITENESS_DELAY)

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