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
import math
import threading
from datetime import datetime, timezone
from pathlib import Path

import anthropic

from astrocyte       import Astrocyte
from site_builder    import build as build_site
from knowledge_graph import get_graph, classify_node_edge
import voice

FREED_DIR       = Path(__file__).parent
PROJECTS_DIR    = FREED_DIR / "docs" / "projects"
PROJECTS_IDX    = FREED_DIR / "docs" / "projects.json"
CONSOLIDATE_LOG = FREED_DIR / "FREED_log" / "consolidations.jsonl"

MODEL       = "claude-sonnet-4-6"
HAIKU_MODEL = "claude-haiku-4-5-20251001"

YIELD_THRESHOLD    = 0.03   # feed yield above this triggers consolidation
CONSOLIDATE_EVERY  = 5      # also consolidate every N daemon cycles
MAX_NODES_PER_PASS = 8      # renormalize at most this many nodes per run
MINE_COMPRESS_CAP  = 500    # chars per node in MINE digest
MINE_INV_CAP       = 15     # invariants per node in MINE digest


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

ESCROW_LEDGER_PATH = FREED_DIR / "FREED_log" / "escrow_ledger.json"


class EscrowLedger:
    """
    Escrow-style obligation tracker.

    Obligations incurred during FEED are registered as escrowed entries.
    They cannot be closed, dropped, or auto-resolved without an explicit
    RESOLVE call that supplies falsifiable evidence. This enforces
    Seed Integrity Rule 3: open obligations are debts, not suggestions.

    Isomorphic to SmartSON's escrow release condition — the "funds" here
    are epistemic commitments, and the "smart contract" is the evidence
    gate that must be satisfied before release.
    """

    def __init__(self):
        self._ledger = self._load()

    def _load(self):
        # type: () -> list
        if ESCROW_LEDGER_PATH.exists():
            try:
                return json.loads(ESCROW_LEDGER_PATH.read_text())
            except (json.JSONDecodeError, OSError):
                return []
        return []

    def _save(self):
        ESCROW_LEDGER_PATH.parent.mkdir(exist_ok=True)
        ESCROW_LEDGER_PATH.write_text(
            json.dumps(self._ledger, indent=2, ensure_ascii=False))

    def escrow(self, obligation_id, obligation_text, source_phase="feed",
               node_id=None, cycle=None):
        # type: (str, str, str, str, int) -> dict
        """Register an obligation into escrow. Returns the escrow entry."""
        entry = {
            "escrow_id":       f"esc_{obligation_id}_{int(time.time())}",
            "obligation_id":   obligation_id,
            "obligation_text": obligation_text,
            "source_phase":    source_phase,
            "node_id":         node_id,
            "cycle":           cycle,
            "status":          "escrowed",       # escrowed | resolved | contested
            "escrowed_at":     datetime.now(timezone.utc).isoformat(),
            "resolved_at":     None,
            "evidence":        None,
            "resolve_source":  None,
        }
        self._ledger.append(entry)
        self._save()
        return entry

    def resolve(self, obligation_id, evidence, resolve_source="consolidate"):
        # type: (str, str, str) -> dict
        """
        Release an obligation from escrow IF evidence is provided.
        Evidence must be a non-empty string describing the falsifiable
        basis for resolution. Returns the updated entry or raises.
        """
        if not evidence or not evidence.strip():
            raise ValueError(
                f"Escrow release DENIED for '{obligation_id}': "
                f"no evidence provided. Obligations cannot be silently closed."
            )

        for entry in self._ledger:
            if (entry["obligation_id"] == obligation_id
                    and entry["status"] == "escrowed"):
                entry["status"]         = "resolved"
                entry["resolved_at"]    = datetime.now(timezone.utc).isoformat()
                entry["evidence"]       = evidence.strip()
                entry["resolve_source"] = resolve_source
                self._save()
                return entry

        raise KeyError(
            f"No escrowed obligation found with id '{obligation_id}'. "
            f"It may have already been resolved or was never escrowed."
        )

    def contest(self, obligation_id, reason):
        # type: (str, str) -> dict
        """Mark an escrowed obligation as contested (not resolved — still open)."""
        for entry in self._ledger:
            if (entry["obligation_id"] == obligation_id
                    and entry["status"] == "escrowed"):
                entry["status"] = "contested"
                entry["evidence"] = f"CONTESTED: {reason}"
                self._save()
                return entry
        raise KeyError(f"No escrowed obligation '{obligation_id}' to contest.")

    def open_escrows(self):
        # type: () -> list
        """Return all obligations still in escrow (not yet resolved)."""
        return [e for e in self._ledger if e["status"] == "escrowed"]

    def stale_escrows(self, max_age_cycles=10, current_cycle=0):
        # type: (int, int) -> list
        """Return escrowed obligations that have been open too long."""
        stale = []
        for e in self._ledger:
            if e["status"] != "escrowed":
                continue
            entry_cycle = e.get("cycle") or 0
            if current_cycle - entry_cycle >= max_age_cycles:
                stale.append(e)
        return stale

    def audit_report(self):
        # type: () -> dict
        """Summary statistics for the escrow ledger."""
        total     = len(self._ledger)
        escrowed  = sum(1 for e in self._ledger if e["status"] == "escrowed")
        resolved  = sum(1 for e in self._ledger if e["status"] == "resolved")
        contested = sum(1 for e in self._ledger if e["status"] == "contested")
        return {
            "total":     total,
            "escrowed":  escrowed,
            "resolved":  resolved,
            "contested": contested,
            "integrity": "CLEAN" if escrowed == 0 else f"OPEN_DEBT({escrowed})",
        }

    def enforce_no_silent_close(self, obligations_list):
        # type: (list) -> list
        """
        Cross-check: given the system's obligations list, find any that were
        marked 'resolved' externally but are still escrowed here (i.e., someone
        tried to close them without going through the escrow gate).
        Returns list of violation descriptions.
        """
        escrowed_ids = {e["obligation_id"] for e in self._ledger
                        if e["status"] == "escrowed"}
        violations = []
        for ob in obligations_list:
            ob_id = ob.get("id", "")
            ob_status = ob.get("status", "")
            if ob_id in escrowed_ids and ob_status in ("resolved", "closed"):
                violations.append(
                    f"ESCROW VIOLATION: obligation '{ob_id}' marked "
                    f"'{ob_status}' externally but still escrowed — "
                    f"no evidence was provided through the escrow gate."
                )
        return violations


class MWDEScorer:
    """
    Minimum Wasserstein Distance Estimator (MWDE) scoring for misspecification-robust
    evidence integration.

    Based on asymptotic theory: when the data-generating process P0 is NOT in the
    model family {P_theta}, the MWDE still converges to the projection
    theta* = argmin_theta W_p(P0, P_theta), and the estimator is asymptotically
    normal around theta* at sqrt(n) rate.

    This means Wasserstein-based scoring degrades gracefully under model
    misspecification — unlike KL-based scoring which can diverge when the
    model family doesn't contain the truth.

    For FREED: we treat each node's semantic distribution (word/concept frequencies)
    as the model distribution, and the empirical evidence (new knowledge) as the
    data distribution. The MWDE score quantifies how well the node's semantic
    model accommodates the evidence, even when the node's model is known to be
    approximate.
    """

    def __init__(self, wasserstein_order=1):
        # type: (int) -> None
        self.p = wasserstein_order  # W_p distance order (1 = Earth Mover's)

    @staticmethod
    def _text_to_distribution(text, min_word_len=4):
        # type: (str, int) -> dict
        """Convert text to a normalized word frequency distribution."""
        stopwords = {"that", "this", "with", "from", "have", "been", "will",
                     "their", "they", "which", "what", "when", "where", "there",
                     "would", "could", "should", "about", "into", "than", "then",
                     "also", "some", "more", "most", "only", "just", "very"}
        words = [
            w.lower().strip(".,;:()[]'\"!?-")
            for w in text.split()
        ]
        words = [w for w in words if len(w) >= min_word_len and w not in stopwords]
        if not words:
            return {}
        counts = {}
        for w in words:
            counts[w] = counts.get(w, 0) + 1
        total = float(sum(counts.values()))
        return {w: c / total for w, c in counts.items()}

    def _discrete_wasserstein_1d(self, dist_a, dist_b):
        # type: (dict, dict) -> float
        """
        Compute discrete W_1 (Earth Mover's Distance) between two distributions
        over a shared vocabulary, using the sorted-CDF method.

        For discrete distributions on a linearly-ordered set, W_1 equals the
        L1 norm of the difference of cumulative distributions. We order
        the shared vocabulary alphabetically as a canonical ordering.

        When supports don't overlap, unmatched mass contributes full cost —
        this is the misspecification-robust property: distant semantic content
        produces high W, not infinity (as KL would).
        """
        all_keys = sorted(set(list(dist_a.keys()) + list(dist_b.keys())))
        if not all_keys:
            return 1.0  # no content = maximal distance

        # Build CDFs
        cdf_a = 0.0
        cdf_b = 0.0
        w1 = 0.0
        for key in all_keys:
            cdf_a += dist_a.get(key, 0.0)
            cdf_b += dist_b.get(key, 0.0)
            w1 += abs(cdf_a - cdf_b)

        # Normalize by vocabulary size to keep score in [0, 1] range
        n_keys = len(all_keys)
        return w1 / n_keys if n_keys > 0 else 1.0

    def score_node_evidence(self, node_text, evidence_text):
        # type: (str, str) -> dict
        """
        Score how well a node's semantic model accommodates new evidence,
        using MWDE theory for misspecification-robust assessment.

        Returns:
            dict with:
                - wasserstein_distance: W_1 between node and evidence distributions
                - mwde_weight: evidence weight (higher = more compatible)
                - misspecification_flag: True if distance suggests model inadequacy
                - asymptotic_variance_proxy: proxy for the MWDE asymptotic variance
                  (larger support overlap = tighter inference)
        """
        dist_node = self._text_to_distribution(node_text)
        dist_evidence = self._text_to_distribution(evidence_text)

        if not dist_node or not dist_evidence:
            return {
                "wasserstein_distance": 1.0,
                "mwde_weight": 0.0,
                "misspecification_flag": True,
                "asymptotic_variance_proxy": float('inf'),
                "support_overlap": 0.0,
            }

        w_dist = self._discrete_wasserstein_1d(dist_node, dist_evidence)

        # Support overlap — proxy for sample size in the MWDE convergence rate
        support_a = set(dist_node.keys())
        support_b = set(dist_evidence.keys())
        overlap = len(support_a & support_b)
        union = len(support_a | support_b)
        support_ratio = overlap / union if union > 0 else 0.0

        # MWDE weight: transform distance to a compatibility score
        # Using exp(-w/scale) which is the natural kernel for W_1 distances
        # Scale parameter controls sensitivity — calibrated so W=0.5 gives ~0.37 weight
        scale = 0.5
        mwde_weight = math.exp(-w_dist / scale)

        # Asymptotic variance proxy: V ~ W^2 / n_overlap
        # When support overlap is small, variance is large (less reliable inference)
        # This follows from the MWDE asymptotic normality: sqrt(n)(theta_hat - theta*) -> N(0, V)
        if overlap > 0:
            avar_proxy = (w_dist ** 2) / overlap
        else:
            avar_proxy = float('inf')

        # Misspecification detection: high distance + low support overlap
        # means the node's model family likely doesn't contain the truth
        misspec_threshold = 0.7
        misspec = w_dist > misspec_threshold and support_ratio < 0.3

        return {
            "wasserstein_distance": round(w_dist, 4),
            "mwde_weight": round(mwde_weight, 4),
            "misspecification_flag": misspec,
            "asymptotic_variance_proxy": round(avar_proxy, 6) if avar_proxy != float('inf') else float('inf'),
            "support_overlap": round(support_ratio, 4),
        }

    def rank_nodes_by_evidence(self, nodes, evidence_text):
        # type: (list, str) -> list
        """
        Rank nodes by MWDE-weighted compatibility with evidence.
        Returns list of (node, mwde_score_dict) tuples, sorted by mwde_weight descending.

        This implements the misspecification-robust ranking: nodes whose semantic
        model is a poor fit for the evidence get low weight rather than being
        excluded entirely (graceful degradation, not hard cutoff).
        """
        scored = []
        for node in nodes:
            node_text = " ".join(filter(None, [
                node.get("compress", ""),
                node.get("summary", ""),
                " ".join(node.get("invariants", [])),
                " ".join(node.get("tags", [])),
            ]))
            mwde = self.score_node_evidence(node_text, evidence_text)
            scored.append((node, mwde))

        scored.sort(key=lambda x: x[1]["mwde_weight"], reverse=True)
        return scored

    def aggregate_evidence_weights(self, mwde_scores):
        # type: (list) -> dict
        """
        Aggregate MWDE scores across multiple nodes to produce a consolidated
        evidence quality assessment.

        Uses inverse-variance weighting from the MWDE asymptotic normality result:
        the optimal combination weights each node's evidence by 1/V_i where V_i
        is the asymptotic variance proxy for that node.
        """
        if not mwde_scores:
            return {"aggregate_weight": 0.0, "n_misspecified": 0, "n_scored": 0}

        total_weight = 0.0
        inv_var_sum = 0.0
        n_misspec = 0
        n_finite = 0

        for s in mwde_scores:
            total_weight += s["mwde_weight"]
            if s["misspecification_flag"]:
                n_misspec += 1
            avar = s["asymptotic_variance_proxy"]
            if avar != float('inf') and avar > 0:
                inv_var_sum += 1.0 / avar
                n_finite += 1

        avg_weight = total_weight / len(mwde_scores)

        # Effective sample size from inverse-variance weighting
        if inv_var_sum > 0 and n_finite > 0:
            eff_sample_size = inv_var_sum  # sum of precisions
        else:
            eff_sample_size = 0.0

        return {
            "aggregate_weight": round(avg_weight, 4),
            "effective_sample_size": round(eff_sample_size, 4),
            "n_misspecified": n_misspec,
            "n_scored": len(mwde_scores),
            "misspecification_ratio": round(n_misspec / len(mwde_scores), 3),
        }


class WassersteinBarycenterAggregator:
    """
    Wasserstein Barycenter aggregation for multi-model belief combination.

    Replaces KL-mixture posterior averaging with W2-optimal barycenter
    computation, preserving geometric structure of the distributional
    support rather than collapsing to a mixture that averages away
    support diversity.

    Based on the Bayesian Wasserstein Barycenter (BWB) framework:
    the barycenter minimizes the weighted sum of W2 distances to the
    input distributions, yielding the Fréchet mean in Wasserstein space.

    For discrete 1D distributions (our case: word frequency distributions
    over a shared vocabulary), the W2 barycenter has a closed-form
    solution via quantile averaging:
        F_bary^{-1}(t) = sum_k lambda_k * F_k^{-1}(t)

    where F_k^{-1} are the quantile functions and lambda_k are weights.

    This is exact in 1D and avoids the computational intractability of
    the general d-dimensional barycenter problem (INV_094 challenge).
    By operating on the quantile representation, we get O(n log n)
    complexity rather than the O(n^3) of general OT solvers.

    For FREED's consolidation pass: each node's semantic distribution
    is a "model posterior" over vocabulary, and the barycenter is the
    geometrically faithful aggregate belief that preserves support
    structure — mass doesn't wash out into a flat mixture.
    """

    def __init__(self, n_quantile_points=200):
        # type: (int) -> None
        self.n_quantile_points = n_quantile_points

    @staticmethod
    def _distribution_to_quantile_function(dist, support_keys):
        # type: (dict, list) -> list
        """
        Convert a discrete distribution over ordered support keys into
        a quantile function (inverse CDF) sampled at uniform points.

        Args:
            dist: dict mapping keys to probabilities (must sum to ~1)
            support_keys: sorted list of all keys in the shared vocabulary

        Returns:
            list of (key_index, cumulative_mass) pairs representing the
            piecewise-constant quantile function
        """
        # Build CDF
        cdf = []
        cumulative = 0.0
        for i, key in enumerate(support_keys):
            cumulative += dist.get(key, 0.0)
            cdf.append((i, cumulative))

        # Normalize in case dist doesn't sum to exactly 1
        if cumulative > 0 and abs(cumulative - 1.0) > 1e-9:
            cdf = [(idx, c / cumulative) for idx, c in cdf]

        return cdf

    def _sample_quantile(self, cdf, n_points):
        # type: (list, int) -> list
        """
        Sample the quantile function (inverse CDF) at n_points uniform
        quantile levels in (0, 1).

        Returns list of key_indices corresponding to each quantile level.
        """
        if not cdf:
            return [0] * n_points

        quantile_levels = [(i + 0.5) / n_points for i in range(n_points)]
        quantiles = []

        cdf_idx = 0
        for t in quantile_levels:
            # Find smallest index where CDF >= t
            while cdf_idx < len(cdf) - 1 and cdf[cdf_idx][1] < t:
                cdf_idx += 1
            quantiles.append(cdf[cdf_idx][0])

        return quantiles

    def compute_barycenter(self, distributions, weights=None):
        # type: (list, list) -> dict
        """
        Compute the Wasserstein-2 barycenter of multiple discrete distributions
        using the 1D quantile averaging closed form.

        Args:
            distributions: list of dicts (key -> probability)
            weights: optional list of floats summing to 1; if None, uniform

        Returns:
            dict mapping vocabulary keys to barycenter probabilities.
            The barycenter is the Fréchet mean in W2 space: it minimizes
            sum_k lambda_k * W2(bary, dist_k)^2.
        """
        if not distributions:
            return {}

        if len(distributions) == 1:
            return dict(distributions[0])

        k = len(distributions)
        if weights is None:
            weights = [1.0 / k] * k
        else:
            # Normalize weights
            w_sum = sum(weights)
            if w_sum > 0:
                weights = [w / w_sum for w in weights]
            else:
                weights = [1.0 / k] * k

        # Build shared vocabulary (sorted for canonical ordering)
        all_keys = set()
        for d in distributions:
            all_keys.update(d.keys())
        support_keys = sorted(all_keys)

        if not support_keys:
            return {}

        n_pts = self.n_quantile_points

        # Compute quantile functions for each distribution
        quantile_functions = []
        for d in distributions:
            cdf = self._distribution_to_quantile_function(d, support_keys)
            qf = self._sample_quantile(cdf, n_pts)
            quantile_functions.append(qf)

        # Barycenter quantile function: weighted average of quantile indices
        # F_bary^{-1}(t) = sum_k lambda_k * F_k^{-1}(t)
        bary_quantiles = []
        for j in range(n_pts):
            weighted_idx = 0.0
            for i in range(k):
                weighted_idx += weights[i] * quantile_functions[i][j]
            bary_quantiles.append(weighted_idx)

        # Convert barycenter quantile function back to a distribution
        # by counting how often each key_index appears in the quantile samples
        key_counts = {}
        for q_idx in bary_quantiles:
            # Round to nearest integer key index
            rounded = int(round(q_idx))
            rounded = max(0, min(rounded, len(support_keys) - 1))
            key = support_keys[rounded]
            key_counts[key] = key_counts.get(key, 0) + 1

        # Normalize to probability distribution
        total = float(sum(key_counts.values()))
        if total > 0:
            bary_dist = {k: v / total for k, v in key_counts.items()}
        else:
            bary_dist = {}

        return bary_dist

    def aggregate_node_beliefs(self, nodes, evidence_text, mwde_scorer):
        # type: (list, str, MWDEScorer) -> dict
        """
        Aggregate multiple nodes' semantic distributions into a single
        Wasserstein barycenter, weighted by MWDE compatibility scores.

        This replaces KL-mixture averaging: instead of
            p_mix = sum_k lambda_k * p_k  (KL mixture — washes out geometry)
        we compute
            p_bary = argmin_q sum_k lambda_k * W2(q, p_k)^2  (preserves support)

        Args:
            nodes: list of node dicts
            evidence_text: the new knowledge being consolidated
            mwde_scorer: MWDEScorer instance for compatibility weighting

        Returns:
            dict with:
                - barycenter: the aggregated distribution
                - weights: the MWDE-derived weights used
                - kl_mixture: the KL-mixture for comparison (diagnostic)
                - support_preservation: ratio of barycenter support size
                  to average input support size (>1 means geometry preserved)
                - w2_cost: total weighted W2 cost of the barycenter
                - n_models: number of input distributions
        """
        if not nodes:
            return {
                "barycenter": {},
                "weights": [],
                "kl_mixture": {},
                "support_preservation": 0.0,
                "w2_cost": 0.0,
                "n_models": 0,
            }

        # Build distributions and compute MWDE weights
        distributions = []
        mwde_weights = []
        for node in nodes:
            node_text = " ".join(filter(None, [
                node.get("compress", ""),
                node.get("summary", ""),
                " ".join(node.get("invariants", [])),
                " ".join(node.get("tags", [])),
            ]))
            dist = MWDEScorer._text_to_distribution(node_text)
            if dist:
                distributions.append(dist)
                score = mwde_scorer.score_node_evidence(node_text, evidence_text)
                mwde_weights.append(score["mwde_weight"])

        if not distributions:
            return {
                "barycenter": {},
                "weights": [],
                "kl_mixture": {},
                "support_preservation": 0.0,
                "w2_cost": 0.0,
                "n_models": 0,
            }

        # Normalize weights
        w_sum = sum(mwde_weights)
        if w_sum > 0:
            norm_weights = [w / w_sum for w in mwde_weights]
        else:
            norm_weights = [1.0 / len(mwde_weights)] * len(mwde_weights)

        # Compute Wasserstein barycenter
        barycenter = self.compute_barycenter(distributions, norm_weights)

        # Compute KL mixture for comparison (diagnostic only)
        kl_mixture = {}
        for i, dist in enumerate(distributions):
            for key, prob in dist.items():
                kl_mixture[key] = kl_mixture.get(key, 0.0) + norm_weights[i] * prob

        # Support preservation metric:
        # ratio of barycenter support size to mean input support size
        # >1.0 means barycenter preserves or extends support (geometric fidelity)
        # <1.0 would mean support collapse (shouldn't happen with OT barycenters)
        input_support_sizes = [len(d) for d in distributions]
        mean_support = sum(input_support_sizes) / len(input_support_sizes) if input_support_sizes else 1.0
        bary_support = len(barycenter)
        support_preservation = bary_support / mean_support if mean_support > 0 else 0.0

        # Compute total W2 cost: sum_k lambda_k * W1(bary, dist_k)
        # (Using W1 as proxy since we have the discrete W1 implementation)
        w1_scorer = MWDEScorer(wasserstein_order=1)
        total_w2_cost = 0.0
        for i, dist in enumerate(distributions):
            w_dist = w1_scorer._discrete_wasserstein_1d(barycenter, dist)
            total_w2_cost += norm_weights[i] * w_dist

        return {
            "barycenter": barycenter,
            "weights": [round(w, 4) for w in norm_weights],
            "kl_mixture": kl_mixture,
            "support_preservation": round(support_preservation, 4),
            "w2_cost": round(total_w2_cost, 6),
            "n_models": len(distributions),
        }


class NonQuadraticEPRScorer:
    """
    Non-quadratic Entropy Production Rate (EPR) action scorer for
    thermodynamic admissibility of knowledge-path merges.

    Implements the paper's large-deviation rate functional:
        φ(x) = x·ln(x) - x + 1
    instead of the near-equilibrium quadratic approximation:
        φ_quad(x) = (x - 1)² / 2

    The non-quadratic form is tight for discrete-state Markov processes
    far from equilibrium, producing strictly tighter bounds on which
    consolidation merges are thermodynamically admissible. At high
    semantic tension (large flux ratios), the quadratic form under-
    estimates dissipation, permitting false consolidations.

    The EPR action for a transition with forward flux j and equilibrium
    flux j_eq is:
        Σ_EPR = Σ_edges  j_eq · φ(j / j_eq)

    where φ(x) = x·ln(x) - x + 1 is the Cramér rate function.

    INV_073 DISCRETE CORRECTION: The paper's geodesic optimum is derived
    for discrete-state systems. When the genome uses continuous Wasserstein
    (W2) geometry, a discrete-state correction term is needed. We apply
    a log-barrier correction: for N discrete states, the effective metric
    gains a factor of (1 + 1/N), which vanishes in the continuous limit
    but tightens the bound for small state spaces.
    """

    # The Cramér / large-deviation rate function
    @staticmethod
    def _phi(x):
        # type: (float) -> float
        """φ(x) = x·ln(x) - x + 1, the non-quadratic dissipation functional.
        Defined for x > 0. φ(0) = 1 by continuous extension. φ(1) = 0."""
        if x <= 0.0:
            return 1.0  # lim_{x->0+} φ(x) = 1
        if abs(x - 1.0) < 1e-12:
            return 0.0  # equilibrium: no dissipation
        return x * math.log(x) - x + 1.0

    @staticmethod
    def _phi_quadratic(x):
        # type: (float) -> float
        """Quadratic (near-equilibrium) approximation: (x-1)²/2.
        Taylor expansion of φ(x) around x=1 to second order."""
        return 0.5 * (x - 1.0) ** 2

    def epr_action(self, flux_ratios, equilibrium_fluxes=None):
        # type: (list, list) -> dict
        """
        Compute the EPR action for a set of transition flux ratios.

        Args:
            flux_ratios: list of j/j_eq ratios for each semantic edge
            equilibrium_fluxes: optional weights (j_eq) per edge; if None,
                               uniform weighting is assumed

        Returns:
            dict with epr_nonquad, epr_quad, tightening_ratio, and
            admissibility assessment
        """
        if not flux_ratios:
            return {
                "epr_nonquad": 0.0,
                "epr_quad": 0.0,
                "tightening_ratio": 1.0,
                "admissible": True,
                "n_edges": 0,
            }

        n = len(flux_ratios)
        if equilibrium_fluxes is None:
            equilibrium_fluxes = [1.0 / n] * n

        # Normalize equilibrium fluxes
        total_jeq = sum(equilibrium_fluxes)
        if total_jeq <= 0:
            total_jeq = 1.0
        jeq_norm = [j / total_jeq for j in equilibrium_fluxes]

        epr_nq = 0.0
        epr_q = 0.0
        for i, x in enumerate(flux_ratios):
            w = jeq_norm[i]
            epr_nq += w * self._phi(x)
            epr_q += w * self._phi_quadratic(x)

        # Tightening ratio: how much tighter the non-quadratic bound is
        # For x > 1: φ(x) > φ_quad(x), so non-quadratic is strictly larger
        # For x < 1: φ(x) > φ_quad(x) as well (both are convex, φ is tighter)
        if epr_q > 1e-12:
            tightening = epr_nq / epr_q
        else:
            tightening = 1.0

        return {
            "epr_nonquad": round(epr_nq, 6),
            "epr_quad": round(epr_q, 6),
            "tightening_ratio": round(tightening, 4),
            "admissible": True,  # assessed by score_merge_admissibility
            "n_edges": n,
        }

    def semantic_flux_ratios(self, dist_node, dist_evidence):
        # type: (dict, dict) -> tuple
        """
        Compute flux ratios j/j_eq from two semantic distributions.

        The node distribution is treated as the equilibrium (reference)
        distribution j_eq, and the evidence distribution as the current
        flux j. The ratio j/j_eq for each shared vocabulary term gives
        the local departure from equilibrium.

        Returns:
            (flux_ratios, equilibrium_fluxes, shared_keys)
        """
        all_keys = sorted(set(list(dist_node.keys()) + list(dist_evidence.keys())))
        if not all_keys:
            return [], [], []

        flux_ratios = []
        eq_fluxes = []

        # Small floor to avoid division by zero — represents minimal
        # background probability (Laplace smoothing analogue)
        floor = 1e-6

        for key in all_keys:
            j_eq = dist_node.get(key, 0.0) + floor
            j = dist_evidence.get(key, 0.0) + floor
            flux_ratios.append(j / j_eq)
            eq_fluxes.append(j_eq)

        return flux_ratios, eq_fluxes, all_keys

    def score_merge_admissibility(self, node_text, evidence_text,
                                  tension_threshold=0.15,
                                  n_discrete_states=None):
        # type: (str, str, float, int) -> dict
        """
        Score whether merging evidence into a node is thermodynamically
        admissible under the non-quadratic EPR action.

        Uses the large-deviation rate functional φ(x) = x·ln(x) - x + 1
        instead of the quadratic (x-1)²/2 to detect false consolidations
        at high semantic tension.

        Args:
            node_text: existing node content
            evidence_text: new knowledge to potentially merge
            tension_threshold: EPR above this flags inadmissible merge
            n_discrete_states: if provided, applies discrete-state correction
                              (INV_073) to the metric

        Returns:
            dict with EPR scores, admissibility flag, and discrete correction
        """
        # Reuse MWDEScorer's text-to-distribution for consistency
        dist_node = MWDEScorer._text_to_distribution(node_text)
        dist_evidence = MWDEScorer._text_to_distribution(evidence_text)

        if not dist_node or not dist_evidence:
            return {
                "epr_nonquad": 1.0,
                "epr_quad": 0.5,
                "tightening_ratio": 2.0,
                "admissible": False,
                "thermodynamic_tension": 1.0,
                "discrete_correction": 1.0,
                "quadratic_would_admit": True,
                "false_consolidation_prevented": True,
            }

        flux_ratios, eq_fluxes, shared_keys = self.semantic_flux_ratios(
            dist_node, dist_evidence)

        epr = self.epr_action(flux_ratios, eq_fluxes)

        # INV_073: Discrete-state correction term
        # For N discrete states, the metric gains factor (1 + 1/N)
        # This corrects the continuous W2 geometry used in the genome
        if n_discrete_states is None:
            n_discrete_states = len(shared_keys)
        if n_discrete_states > 0:
            discrete_correction = 1.0 + 1.0 / n_discrete_states
        else:
            discrete_correction = 2.0  # maximal correction for trivial state space

        # Apply discrete correction to the EPR threshold
        effective_threshold = tension_threshold * discrete_correction

        # Thermodynamic tension: normalized EPR that accounts for
        # the number of semantic edges (intensive quantity)
        n_edges = max(len(flux_ratios), 1)
        tension = epr["epr_nonquad"]  # already weighted by eq_fluxes

        # Admissibility under non-quadratic vs quadratic
        admissible_nq = tension < effective_threshold
        admissible_q = epr["epr_quad"] < effective_threshold

        # False consolidation: quadratic says yes, non-quadratic says no
        false_consolidation_prevented = admissible_q and not admissible_nq

        return {
            "epr_nonquad": epr["epr_nonquad"],
            "epr_quad": epr["epr_quad"],
            "tightening_ratio": epr["tightening_ratio"],
            "admissible": admissible_nq,
            "thermodynamic_tension": round(tension, 6),
            "effective_threshold": round(effective_threshold, 6),
            "discrete_correction": round(discrete_correction, 6),
            "n_semantic_edges": n_edges,
            "quadratic_would_admit": admissible_q,
            "false_consolidation_prevented": false_consolidation_prevented,
        }

    def thermodynamic_length(self, path_distributions):
        # type: (list) -> dict
        """
        Compute the thermodynamic length of a knowledge path (sequence of
        distributions), using the non-quadratic EPR action as the local
        metric.

        The paper proves that EPR-minimizing trajectories are geodesics
        under this thermodynamic length (Noether: action invariance under
        reparametrization). This provides a conserved quantity along
        optimal consolidation paths.

        Args:
            path_distributions: list of distribution dicts (word->freq)
                               representing the knowledge path

        Returns:
            dict with total thermodynamic length and per-step EPR
        """
        if len(path_distributions) < 2:
            return {"thermodynamic_length": 0.0, "steps": [], "n_steps": 0}

        steps = []
        total_length = 0.0

        for i in range(len(path_distributions) - 1):
            dist_a = path_distributions[i]
            dist_b = path_distributions[i + 1]

            flux_ratios, eq_fluxes, _ = self.semantic_flux_ratios(dist_a, dist_b)
            epr = self.epr_action(flux_ratios, eq_fluxes)

            # Thermodynamic length element: sqrt(EPR) for the Riemannian
            # metric induced by the Fisher-Rao / EPR structure
            dl = math.sqrt(epr["epr_nonquad"]) if epr["epr_nonquad"] > 0 else 0.0
            total_length += dl

            steps.append({
                "step": i,
                "epr": epr["epr_nonquad"],
                "dl": round(dl, 6),
            })

        return {
            "thermodynamic_length": round(total_length, 6),
            "steps": steps,
            "n_steps": len(steps),
        }


ENERGY_CORRECTION_LOG = FREED_DIR / "FREED_log" / "energy_corrections.jsonl"


class EnergyCorrection:
    """
    EOP-SAV–inspired energy-correction audit for coherence scoring.

    The EOP-SAV result proves that surrogate-energy preservation (e.g.,
    the SAV modified energy) drifts cumulatively from the true dissipation
    structure unless an explicit correction step is applied each cycle.

    FREED's coherence_score is exactly such a surrogate: it is updated via
    LLM-estimated deltas (a surrogate metric) rather than computed from the
    true epistemic free energy. Over 238+ generations of recursive
    compression, this introduces the same cumulative drift the EOP-SAV
    paper identifies.

    True epistemic free energy for a node:
        E_true = grounding × falsification_load

    where:
        grounding = (confirmed_invariants) / (confirmed_invariants + open_obligations)
        falsification_load = 1 - (contested_or_failed / total_obligations)  if any, else 1.0

    The correction gap:
        gap = |coherence_score_surrogate - E_true|

    When gap > threshold, the cycle is flagged: the surrogate has drifted
    too far from the true energy and coherence_score should be corrected.

    Relaxation correction (RSAV-style):
        coherence_corrected = ξ · coherence_surrogate + (1 - ξ) · E_true

    where ξ ∈ [0, 1] is the relaxation parameter. ξ = 1 means no correction
    (pure surrogate), ξ = 0 means full replacement with E_true. We use
    ξ = 0.5 by default — equal weighting of surrogate dynamics and true energy.
    """

    DEFAULT_GAP_THRESHOLD = 0.12   # flag if |surrogate - E_true| exceeds this
    DEFAULT_RELAXATION_XI = 0.5    # relaxation parameter ξ

    def __init__(self, gap_threshold=None, relaxation_xi=None):
        # type: (float, float) -> None
        self.gap_threshold = gap_threshold if gap_threshold is not None else self.DEFAULT_GAP_THRESHOLD
        self.relaxation_xi = relaxation_xi if relaxation_xi is not None else self.DEFAULT_RELAXATION_XI

    @staticmethod
    def _compute_grounding(node):
        # type: (dict) -> float
        """
        Grounding = confirmed_invariants / (confirmed_invariants + open_obligations).

        A node with many invariants and few obligations is well-grounded.
        A node with many open obligations relative to invariants is under-grounded.
        """
        n_invariants = len(node.get("invariants", []))
        obligations = node.get("obligations", [])
        # Count obligations: if they're strings, each is one; if dicts, count them
        if obligations and isinstance(obligations[0], dict):
            n_obligations = sum(
                1 for o in obligations
                if o.get("status", "open") in ("open", "partial", "escrowed")
            )
        else:
            n_obligations = len(obligations)

        total = n_invariants + n_obligations
        if total == 0:
            return 0.5  # no evidence either way — neutral grounding
        return n_invariants / total

    @staticmethod
    def _compute_falsification_load(node):
        # type: (dict) -> float
        """
        Falsification load = 1 - (contested_or_failed / total_obligations).

        Measures how much of the node's obligation structure has survived
        falsification attempts. A node with no contested obligations has
        load = 1.0 (fully intact). A node where all obligations are contested
        has load = 0.0 (fully eroded).

        If there are no obligations at all, load = 1.0 (nothing to falsify).
        """
        obligations = node.get("obligations", [])
        if not obligations:
            return 1.0

        if obligations and isinstance(obligations[0], dict):
            total = len(obligations)
            contested = sum(
                1 for o in obligations
                if o.get("status", "") in ("contested", "failed", "refuted")
            )
        else:
            # String obligations — no status info, assume all open
            total = len(obligations)
            contested = 0

        if total == 0:
            return 1.0
        return 1.0 - (contested / total)

    def compute_true_energy(self, node):
        # type: (dict) -> float
        """
        True epistemic free energy: E_true = grounding × falsification_load.

        This is the product of two [0,1] quantities, yielding a [0,1] score
        that represents the node's genuine epistemic health — not a surrogate
        updated by LLM delta estimates.
        """
        grounding = self._compute_grounding(node)
        fals_load = self._compute_falsification_load(node)
        return grounding * fals_load

    @staticmethod
    def _estimate_spectral_asymmetry(node):
        # type: (dict) -> float
        """
        Estimate spectral asymmetry γ from a node's evidence density.

        γ measures how directional (non-Hermitian) the evidence is:
        nodes with many confirmed invariants relative to open obligations
        have high γ — their evidence points consistently in one direction.
        Nodes with balanced confirmed/open counts have low γ (symmetric).

        γ = |n_confirmed - n_open| / (n_confirmed + n_open + 1)

        The +1 floor prevents division by zero and ensures γ ∈ [0, 1).
        A node with 5 invariants and 0 obligations has γ ≈ 0.83 (highly
        directional). A node with 3 invariants and 3 obligations has γ = 0
        (symmetric, no preferred direction).
        """
        n_invariants = len(node.get("invariants", []))
        obligations = node.get("obligations", [])
        if obligations and isinstance(obligations[0], dict):
            n_open = sum(
                1 for o in obligations
                if o.get("status", "open") in ("open", "partial", "escrowed")
            )
        else:
            n_open = len(obligations)

        total = n_invariants + n_open + 1  # +1 floor
        gamma = abs(n_invariants - n_open) / total
        return gamma

    def audit_node(self, node):
        # type: (dict) -> dict
        """
        Compute the energy-correction audit for a single node.

        Returns a dict with:
            - e_true: true epistemic free energy
            - e_surrogate: current coherence_score (the surrogate)
            - gap: |e_surrogate - e_true|
            - gap_exceeds_threshold: bool
            - corrected_score: RSAV-relaxation-corrected coherence score
            - grounding: the grounding component
            - falsification_load: the falsification load component
            - correction_applied: whether the gap exceeded threshold
            - spectral_asymmetry: estimated γ for this node
            - effective_gap_threshold: W_c ∝ √γ disorder-tolerance threshold
        """
        e_surrogate = float(node.get("coherence_score", 0.5))
        grounding = self._compute_grounding(node)
        fals_load = self._compute_falsification_load(node)
        e_true = grounding * fals_load

        gap = abs(e_surrogate - e_true)

        # W_c ∝ √γ disorder-tolerance scaling (non-Hermitian topology result):
        # Obligations with higher spectral asymmetry (more directional evidence)
        # tolerate more disorder before being marked unresolved. The critical
        # disorder strength scales as W_c = base_threshold * (1 + √γ), so nodes
        # with high γ get a larger effective noise floor. The (1 + √γ) form
        # ensures the base threshold is always the minimum (when γ=0, symmetric
        # evidence, no bonus tolerance).
        gamma = self._estimate_spectral_asymmetry(node)
        effective_threshold = self.gap_threshold * (1.0 + math.sqrt(gamma))

        exceeds = gap > effective_threshold

        # RSAV-style relaxation correction:
        # coherence_corrected = ξ · e_surrogate + (1 - ξ) · e_true
        xi = self.relaxation_xi
        corrected = xi * e_surrogate + (1.0 - xi) * e_true
        corrected = round(min(0.99, max(0.0, corrected)), 4)

        return {
            "e_true": round(e_true, 4),
            "e_surrogate": round(e_surrogate, 4),
            "gap": round(gap, 4),
            "gap_exceeds_threshold": exceeds,
            "corrected_score": corrected,
            "grounding": round(grounding, 4),
            "falsification_load": round(fals_load, 4),
            "correction_applied": exceeds,
            "relaxation_xi": xi,
            "spectral_asymmetry": round(gamma, 4),
            "effective_gap_threshold": round(effective_threshold, 4),
        }

    def audit_cycle(self, nodes, cycle_number=0):
        # type: (list, int) -> dict
        """
        Run energy-correction audit across all nodes after a FEED cycle.

        Returns a cycle-level report with per-node audits and aggregate stats.
        Flags the cycle if any node's gap exceeds threshold.
        """
        audits = []
        flagged_nodes = []

        for node in nodes:
            node_id = node.get("id", "unknown")
            audit = self.audit_node(node)
            audit["node_id"] = node_id
            audits.append(audit)

            if audit["gap_exceeds_threshold"]:
                flagged_nodes.append({
                    "node_id": node_id,
                    "gap": audit["gap"],
                    "e_surrogate": audit["e_surrogate"],
                    "e_true": audit["e_true"],
                    "corrected_score": audit["corrected_score"],
                })

        # Aggregate stats
        gaps = [a["gap"] for a in audits]
        mean_gap = sum(gaps) / len(gaps) if gaps else 0.0
        max_gap = max(gaps) if gaps else 0.0
        n_flagged = len(flagged_nodes)

        report = {
            "cycle": cycle_number,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "n_nodes_audited": len(audits),
            "n_flagged": n_flagged,
            "mean_gap": round(mean_gap, 4),
            "max_gap": round(max_gap, 4),
            "cycle_flagged": n_flagged > 0,
            "flagged_nodes": flagged_nodes,
            "gap_threshold": self.gap_threshold,
            "relaxation_xi": self.relaxation_xi,
        }

        # Log the audit
        self._log(report)

        if n_flagged > 0:
            print(f"  [ENERGY-AUDIT] ⚠ CYCLE {cycle_number} FLAGGED: "
                  f"{n_flagged}/{len(audits)} node(s) exceed gap threshold "
                  f"(max_gap={max_gap:.4f}, threshold={self.gap_threshold})")
            for fn in flagged_nodes:
                print(f"    → {fn['node_id'][:40]}: surrogate={fn['e_surrogate']:.3f} "
                      f"true={fn['e_true']:.3f} gap={fn['gap']:.4f} "
                      f"→ corrected={fn['corrected_score']:.3f}")
        else:
            print(f"  [ENERGY-AUDIT] Cycle {cycle_number} CLEAN: "
                  f"mean_gap={mean_gap:.4f}, max_gap={max_gap:.4f}")

        return report

    def apply_corrections(self, nodes):
        # type: (list) -> list
        """
        Apply RSAV-relaxation corrections to nodes whose gap exceeds threshold.
        Mutates node dicts in-place (coherence_score updated) and returns
        list of corrected node IDs.
        """
        corrected_ids = []
        for node in nodes:
            audit = self.audit_node(node)
            if audit["correction_applied"]:
                old_score = node.get("coherence_score")
                node["coherence_score"] = audit["corrected_score"]
                node["energy_correction"] = {
                    "old_surrogate": audit["e_surrogate"],
                    "e_true": audit["e_true"],
                    "gap": audit["gap"],
                    "corrected_to": audit["corrected_score"],
                    "relaxation_xi": audit["relaxation_xi"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                corrected_ids.append(node.get("id", "unknown"))
        return corrected_ids

    @staticmethod
    def _log(report):
        # type: (dict) -> None
        ENERGY_CORRECTION_LOG.parent.mkdir(exist_ok=True)
        with open(ENERGY_CORRECTION_LOG, "a") as f:
            f.write(json.dumps(report) + "\n")


class Consolidator:
    def __init__(self, api_key: str):
        self.client    = anthropic.Anthropic(api_key=api_key)
        self.astrocyte = Astrocyte()
        self.escrow    = EscrowLedger()
        self.mwde      = MWDEScorer(wasserstein_order=1)
        self.epr       = NonQuadraticEPRScorer()
        self.energy_correction = EnergyCorrection()

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
            system=[
                {"type": "text", "text": RENORM_SYSTEM,
                 "cache_control": {"type": "ephemeral", "ttl": "1h"}},
            ],
            messages=[{"role": "user", "content": prompt}],
            timeout=90,
        )
        raw = message.content[0].text.strip()
        self.astrocyte.record_usage(
            message.usage.input_tokens,
            message.usage.output_tokens,
            cache_creation_tokens=getattr(message.usage, "cache_creation_input_tokens", 0) or 0,
            cache_read_tokens=getattr(message.usage, "cache_read_input_tokens", 0) or 0,
        )

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

        def _mine_inv_text(invs):
            clipped = [s[:100] for s in invs[:MINE_INV_CAP]]
            return ", ".join(clipped)

        digest = "\n\n".join(
            f"NODE: {n['id']}\n"
            f"COMPRESS: {n.get('compress','')[:MINE_COMPRESS_CAP]}\n"
            f"INVARIANTS: {_mine_inv_text(n.get('invariants', []))}"
            for n in nodes
        )

        message = self._api_call(
            model=HAIKU_MODEL,
            max_tokens=1500,
            system=[
                {"type": "text", "text": MINE_SYSTEM,
                 "cache_control": {"type": "ephemeral", "ttl": "1h"}},
            ],
            messages=[{"role": "user", "content": digest}],
            timeout=90,
        )
        raw = message.content[0].text.strip()
        self.astrocyte.record_usage(
            message.usage.input_tokens,
            message.usage.output_tokens,
            cache_creation_tokens=getattr(message.usage, "cache_creation_input_tokens", 0) or 0,
            cache_read_tokens=getattr(message.usage, "cache_read_input_tokens", 0) or 0,
        )

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

                # Node-to-node edges for shared invariants — classify by relationship type
                graph = get_graph()
                for c in candidates:
                    nodes_in = c.get('appears_in', [])
                    edge_type = classify_node_edge(c['invariant'])
                    # independent_confirmation is reserved for bootstrap CONVERGE only,
                    # never MINE keyword-match. Hard guard regardless of classify result.
                    if edge_type == "independent_confirmation":
                        edge_type = "consistent_with"
                    for i in range(len(nodes_in)):
                        for j in range(i + 1, len(nodes_in)):
                            graph.record_node_edge(
                                nodes_in[i], nodes_in[j],
                                edge_type,
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
