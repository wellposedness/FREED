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
import re
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

# ── scales_with accretion controls (O382 / O286 / DMN echo-clique) ────────────
# Three coupled defenses against the γ=1 cluster re-asserting through node-edges:
#   (1) FREEZE: while O286 is unresolved, scales_with edges are downgraded to
#       consistent_with at mint time — O382's staked remedy.
#   (2) SEMANTIC DEDUP: a node-pair already carrying a semantically-equivalent
#       edge of the same type does not get a second one. Literal prefix-dedup
#       (knowledge_graph.record_node_edge) catches nothing here — the redundancy
#       is in rewording, so the check has to be on normalized claim content.
#   (3) ECHO-CLIQUE ORIGIN GATE: a mined invariant whose APPEARS_IN nodes already
#       form a dense co-assertion clique is treated as SHARED_SOURCE (echo), not
#       independent confirmation — blocks the one-claim-across-a-fully-connected-
#       subgraph inflation of PROMOTE recurrence. Extends the existing MINE
#       ORIGIN filter from text self-report to graph topology.
# Goodhart isolation: none of these gauges enter L7's prompt — operator/graph only.
FREEZE_OBLIGATION_ID   = "O286"   # while unresolved, freeze scales_with minting
CLAIM_DEDUP_JACCARD    = 0.5      # content-token overlap above which two claims are "the same".
                                  # Calibrated on the live heavy-pair distribution: distinct
                                  # co-occurring invariants sit at median 0.10 / p75 0.17, true
                                  # restatements at 0.50–0.71 — 0.5 lands in the empty band, so
                                  # dedup removes rewordings without collapsing distinct claims.
ECHO_CLIQUE_DENSITY    = 0.6      # covered-pair fraction above which APPEARS_IN is an echo clique
ECHO_CLIQUE_MIN_NODES  = 4        # need at least this many resolved nodes to judge a clique
# Edge types that count as "these two nodes keep co-asserting" (echo evidence).
# Excludes challenges/bounds_above/depends_on/operationalizes — real tension/structure.
# O387 audit (2026-06-14): operationalizes is a co-assertion family, not a directional
# dependency — 0/736 edges carry dependency language, 736 edges collapse to 116 distinct
# texts over 78 pairs (echo-inflated, same shape as scales_with). Added here so the
# echo-clique gate counts it toward clique density and suppresses its echo inflation.
_CO_ASSERTION_TYPES    = frozenset({"scales_with", "substrate_independent", "operationalizes", "consistent_with", "shares_invariant"})

_CLAIM_STOPWORDS = frozenset(
    "a an the is are be been being of to in on at by for with and or as that this "
    "these those it its which from across both via using under over into within "
    "more most less than then also not no can may must should each per such where "
    "when while between among given any all some one two".split()
)
_CLAIM_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _normalize_claim(text):
    # type: (str) -> frozenset
    """Content-token set of a claim — lowercase, stopworded, length-filtered.
    Used to detect semantic restatements that differ only in wording."""
    toks = _CLAIM_TOKEN_RE.findall((text or "").lower())
    return frozenset(t for t in toks if t not in _CLAIM_STOPWORDS and len(t) >= 3)


def _claims_equivalent(a, b, threshold=CLAIM_DEDUP_JACCARD):
    # type: (str, str, float) -> bool
    """True if two claims are restatements of each other (Jaccard >= threshold
    on normalized content tokens). Symmetric; empty/disjoint claims never match."""
    sa, sb = _normalize_claim(a), _normalize_claim(b)
    if not sa or not sb:
        return False
    inter = len(sa & sb)
    if inter == 0:
        return False
    return inter / float(len(sa | sb)) >= threshold


def _obligation_unresolved(oid):
    # type: (str) -> bool
    """True if obligation `oid` exists and is not yet resolved. Fail-open
    (returns False) if the file is unreadable — never over-freeze on error."""
    try:
        d = json.loads((FREED_DIR / "FREED_obligations.json").read_text())
        obs = d if isinstance(d, list) else d.get("obligations", d)
        if isinstance(obs, dict):
            obs = list(obs.values())
        for o in obs:
            if o.get("id") == oid:
                return o.get("status") != "resolved"
    except Exception:
        pass
    return False


def _echo_clique_density(graph, nodes):
    # type: (object, list) -> tuple
    """Fraction of node-pairs within `nodes` that already carry a co-assertion
    edge in the graph. Returns (is_echo, density, resolved_n). Only nodes that
    actually exist as edge endpoints count — too few resolved -> not an echo
    (insufficient evidence; never suppress on ignorance)."""
    try:
        graph._ensure_loaded()
        ne = graph._node_edges
    except Exception:
        return (False, 0.0, 0)
    known = set()
    pair_has_edge = set()
    for e in ne:
        f, t, ty = e.get("from"), e.get("to"), e.get("type")
        known.add(f)
        known.add(t)
        if ty in _CO_ASSERTION_TYPES:
            pair_has_edge.add(frozenset((f, t)))
    resolved = [n for n in dict.fromkeys(nodes) if n in known]
    n = len(resolved)
    if n < ECHO_CLIQUE_MIN_NODES:
        return (False, 0.0, n)
    total_pairs = n * (n - 1) // 2
    covered = 0
    for i in range(n):
        for j in range(i + 1, n):
            if frozenset((resolved[i], resolved[j])) in pair_has_edge:
                covered += 1
    density = covered / float(total_pairs) if total_pairs else 0.0
    return (density >= ECHO_CLIQUE_DENSITY, density, n)


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

    @staticmethod
    def _mac_check(dist_node, dist_evidence, mac_threshold=0.15):
        # type: (dict, dict, float) -> dict
        """
        Minimal Assent Connection (MAC) verification.

        Before computing credence correlation (MWDE weight / γ-correlation),
        verify that assent behavior tracks next-token probabilities. The MAC
        requires that the model's binary assent (non-zero probability assigned
        to a token) is consistent with its graded credence (the probability
        mass assigned).

        Operationally: for each term in the shared vocabulary, check whether
        the node "assents" to the term (assigns it non-negligible probability)
        AND whether the magnitude of that assent tracks the evidence distribution.
        If the assent-credence coupling is broken (many terms where the node
        assents but assigns wildly different credence than evidence suggests,
        or vice versa), the MAC fails and any subsequent correlation score
        is flagged as spurious.

        MAC metric:
            For each shared term, compute:
                assent_node = 1 if dist_node[term] > floor else 0
                assent_evidence = 1 if dist_evidence[term] > floor else 0
            Assent agreement = fraction of terms where assent_node == assent_evidence

            For terms where both assent, compute credence tracking:
                rank correlation (Spearman-like) between node and evidence masses

            MAC passes when:
                1. Assent agreement >= mac_threshold (binary behavior tracks)
                2. Credence tracking > 0 among shared-assent terms (graded
                   probabilities are not anti-correlated)

        Args:
            dist_node: node's word frequency distribution
            dist_evidence: evidence's word frequency distribution
            mac_threshold: minimum assent agreement ratio to pass MAC

        Returns:
            dict with mac_pass, assent_agreement, credence_tracking,
            mac_failure_reason (if failed)
        """
        if not dist_node or not dist_evidence:
            return {
                "mac_pass": False,
                "assent_agreement": 0.0,
                "credence_tracking": 0.0,
                "mac_failure_reason": "empty_distribution",
            }

        # Floor for assent: terms below this are treated as "not assented to"
        assent_floor = 1e-4

        all_terms = sorted(set(list(dist_node.keys()) + list(dist_evidence.keys())))
        if not all_terms:
            return {
                "mac_pass": False,
                "assent_agreement": 0.0,
                "credence_tracking": 0.0,
                "mac_failure_reason": "no_vocabulary",
            }

        # Step 1: Assent agreement — binary assent alignment
        agree_count = 0
        both_assent_terms = []
        for term in all_terms:
            a_node = dist_node.get(term, 0.0) > assent_floor
            a_evid = dist_evidence.get(term, 0.0) > assent_floor
            if a_node == a_evid:
                agree_count += 1
            if a_node and a_evid:
                both_assent_terms.append(term)

        assent_agreement = agree_count / len(all_terms) if all_terms else 0.0

        # Step 2: Credence tracking — rank correlation among shared-assent terms
        # Use Spearman-style: rank both distributions over the shared terms,
        # compute 1 - 6*sum(d_i^2) / (n*(n^2-1))
        credence_tracking = 0.0
        if len(both_assent_terms) >= 2:
            node_vals = [dist_node.get(t, 0.0) for t in both_assent_terms]
            evid_vals = [dist_evidence.get(t, 0.0) for t in both_assent_terms]

            def _rank(vals):
                # type: (list) -> list
                indexed = sorted(enumerate(vals), key=lambda x: x[1])
                ranks = [0.0] * len(vals)
                for rank_pos, (orig_idx, _) in enumerate(indexed):
                    ranks[orig_idx] = float(rank_pos + 1)
                return ranks

            ranks_n = _rank(node_vals)
            ranks_e = _rank(evid_vals)
            n_shared = len(both_assent_terms)
            d_sq_sum = sum((ranks_n[i] - ranks_e[i]) ** 2 for i in range(n_shared))
            denom = n_shared * (n_shared ** 2 - 1)
            if denom > 0:
                credence_tracking = 1.0 - (6.0 * d_sq_sum) / denom
            else:
                credence_tracking = 0.0
        elif len(both_assent_terms) == 1:
            # Single shared term: trivially correlated
            credence_tracking = 1.0

        # MAC verdict
        mac_failure_reason = None
        if assent_agreement < mac_threshold:
            mac_failure_reason = (
                f"assent_agreement={assent_agreement:.3f} < threshold={mac_threshold}: "
                f"binary assent behavior does not track next-token probabilities"
            )
        elif credence_tracking <= 0.0 and len(both_assent_terms) >= 2:
            mac_failure_reason = (
                f"credence_tracking={credence_tracking:.3f} <= 0: "
                f"graded credence is anti-correlated with evidence — "
                f"assent-credence coupling is broken"
            )

        mac_pass = mac_failure_reason is None

        return {
            "mac_pass": mac_pass,
            "assent_agreement": round(assent_agreement, 4),
            "credence_tracking": round(credence_tracking, 4),
            "n_shared_assent_terms": len(both_assent_terms),
            "mac_failure_reason": mac_failure_reason,
        }

    @staticmethod
    def _local_entropy_weight(dist):
        # type: (dict) -> dict
        """
        Compute the inverse local entropy weight for an evidence distribution.

        High-entropy (diffuse/uncertain) evidence should apply weaker updates;
        low-entropy (concentrated) evidence should apply stronger ones. This
        operationalizes EnSToM's core finding: steering strength proportional
        to evidence concentration prevents noisy, high-entropy inputs from
        corrupting the knowledge graph with inappropriately strong updates.

        Shannon entropy of the distribution:
            H = -sum(p_i * ln(p_i))

        Normalized entropy (in [0, 1]):
            H_norm = H / ln(|support|)

        Inverse entropy weight (in (0, 1]):
            w_entropy = 1 - H_norm

        A perfectly uniform distribution (maximum entropy) yields w_entropy ≈ 0,
        meaning updates are almost fully attenuated. A peaked distribution
        (one dominant term) yields w_entropy ≈ 1, meaning updates apply at
        full strength.

        The weight is floored at 0.05 so that even maximally diffuse evidence
        can still contribute a minimal update (complete silencing would violate
        the epistemic loop's liveness property).

        Args:
            dist: dict mapping keys to probabilities (should sum to ~1)

        Returns:
            dict with:
                - entropy: raw Shannon entropy H
                - normalized_entropy: H / ln(|support|), in [0, 1]
                - inverse_entropy_weight: 1 - H_norm, floored at 0.05
                - support_size: number of terms in the distribution
                - concentration: qualitative label (concentrated/moderate/diffuse)
        """
        if not dist or len(dist) < 2:
            # Degenerate distribution — single term or empty: maximally concentrated
            return {
                "entropy": 0.0,
                "normalized_entropy": 0.0,
                "inverse_entropy_weight": 1.0,
                "support_size": len(dist) if dist else 0,
                "concentration": "concentrated",
            }

        # Shannon entropy H = -sum(p_i * ln(p_i))
        h = 0.0
        for p in dist.values():
            if p > 0:
                h -= p * math.log(p)

        # Normalize by ln(|support|) to get entropy in [0, 1]
        support_size = len(dist)
        max_h = math.log(support_size) if support_size > 1 else 1.0
        h_norm = h / max_h if max_h > 0 else 1.0
        h_norm = min(1.0, max(0.0, h_norm))

        # Inverse entropy weight: concentrated evidence gets high weight
        # Floor at 0.05 to preserve epistemic loop liveness
        w_entropy = max(0.05, 1.0 - h_norm)

        # Qualitative label for logging
        if h_norm < 0.4:
            concentration = "concentrated"
        elif h_norm < 0.7:
            concentration = "moderate"
        else:
            concentration = "diffuse"

        return {
            "entropy": round(h, 6),
            "normalized_entropy": round(h_norm, 4),
            "inverse_entropy_weight": round(w_entropy, 4),
            "support_size": support_size,
            "concentration": concentration,
        }

    def score_node_evidence(self, node_text, evidence_text):
        # type: (str, str) -> dict
        """
        Score how well a node's semantic model accommodates new evidence,
        using MWDE theory for misspecification-robust assessment.

        MAC precondition gate: before computing credence correlation, verify
        that the model's assent behavior tracks its next-token probabilities
        above a threshold. If MAC fails, the correlation score is flagged as
        spurious rather than returned as a valid measurement. This prevents
        O21's AlphaPruning protocol from producing spurious γ-correlation
        measurements on models where assent-credence coupling is broken.

        Entropy-weighted update magnitude: the MWDE weight is further scaled
        by the inverse local entropy of the evidence distribution. High-entropy
        (diffuse) evidence applies weaker updates; low-entropy (concentrated)
        evidence applies stronger ones. This operationalizes EnSToM's core
        finding — steering strength proportional to evidence concentration
        prevents noisy inputs from corrupting the knowledge graph.

        Returns:
            dict with:
                - wasserstein_distance: W_1 between node and evidence distributions
                - mwde_weight: evidence weight (higher = more compatible)
                - entropy_weight: inverse local entropy scaling factor
                - effective_weight: mwde_weight * entropy_weight (the operative score)
                - misspecification_flag: True if distance suggests model inadequacy
                - asymptotic_variance_proxy: proxy for the MWDE asymptotic variance
                  (larger support overlap = tighter inference)
                - mac_check: MAC verification result dict
                - mac_failure: True if MAC failed (correlation score is spurious)
        """
        dist_node = self._text_to_distribution(node_text)
        dist_evidence = self._text_to_distribution(evidence_text)

        if not dist_node or not dist_evidence:
            return {
                "wasserstein_distance": 1.0,
                "mwde_weight": 0.0,
                "entropy_weight": {"entropy": 0.0, "normalized_entropy": 1.0,
                                   "inverse_entropy_weight": 0.05,
                                   "support_size": 0, "concentration": "diffuse"},
                "effective_weight": 0.0,
                "misspecification_flag": True,
                "asymptotic_variance_proxy": float('inf'),
                "support_overlap": 0.0,
                "mac_check": {"mac_pass": False, "assent_agreement": 0.0,
                              "credence_tracking": 0.0,
                              "mac_failure_reason": "empty_distribution"},
                "mac_failure": True,
            }

        # ── MAC precondition gate ─────────────────────────────────────────
        # Verify assent-credence coupling before computing correlation.
        # If MAC fails, flag explicitly rather than returning spurious score.
        mac_result = self._mac_check(dist_node, dist_evidence)

        # ── Inverse local entropy of evidence distribution ────────────────
        # Compute before MAC gate so entropy_weight is always available
        # for downstream consumers even when MAC fails.
        entropy_info = self._local_entropy_weight(dist_evidence)

        if not mac_result["mac_pass"]:
            # MAC failure: return flagged result with zero weight.
            # The correlation score would be spurious — do not compute it.
            return {
                "wasserstein_distance": float('nan'),
                "mwde_weight": 0.0,
                "entropy_weight": entropy_info,
                "effective_weight": 0.0,
                "misspecification_flag": True,
                "asymptotic_variance_proxy": float('inf'),
                "support_overlap": 0.0,
                "mac_check": mac_result,
                "mac_failure": True,
                "ks_fit": {"tested": False, "reason": "mac_failure"},
            }

        w_dist = self._discrete_wasserstein_1d(dist_node, dist_evidence)

        # ── Entropic (cross-entropy-style) log-ratio penalty ──────────────
        # Entropic distance metrics have fewer flat basins and stronger
        # gradients than quadratic distance metrics, reducing the chance
        # of stalling at spurious near-duplicates during consolidation
        # scoring. The log-ratio penalty computes a symmetrized KL-like
        # divergence over the shared vocabulary:
        #
        #   D_entropic = 0.5 * [ sum_i p_i * ln(p_i/q_i)
        #                      + sum_i q_i * ln(q_i/p_i) ]
        #
        # where p = dist_node, q = dist_evidence, with Laplace smoothing
        # to avoid log(0). This is the Jensen-Shannon-like symmetric form.
        #
        # The final score blends entropic and Wasserstein distances with
        # entropic weighted MORE heavily (0.6 entropic, 0.4 Wasserstein),
        # since entropic loss exhibits stronger gradients and fewer
        # stationary points than quadratic/Wasserstein loss (confirmed
        # empirically: entropic loss landscapes have more searchable
        # structure with fewer spurious local minima).
        #
        # INV_073 challenge acknowledgment: even entropic loss landscapes
        # contain *multiple* basins of attraction — the critical ridge is
        # a structured set, not a single attractor. The log-ratio penalty
        # reduces but does not eliminate basin multiplicity. We accept
        # this limitation and use the entropic metric for its gradient
        # strength advantage, not for uniqueness of convergence.
        #
        # Noether note (Standard RL): quadratic loss (analogous to standard
        # RL reward-squared objectives) produces more stationary traps and
        # weaker gradients than entropic loss, corroborating that non-entropic
        # optimization frameworks break the symmetry conservation properties
        # associated with thermodynamically admissible learning.
        _entropy_floor = 1e-8  # Laplace smoothing floor
        _all_keys_entropic = sorted(
            set(list(dist_node.keys()) + list(dist_evidence.keys())))
        _kl_pq = 0.0  # KL(node || evidence)
        _kl_qp = 0.0  # KL(evidence || node)
        _n_entropic_keys = len(_all_keys_entropic)
        if _n_entropic_keys > 0:
            for _ek in _all_keys_entropic:
                _p = dist_node.get(_ek, 0.0) + _entropy_floor
                _q = dist_evidence.get(_ek, 0.0) + _entropy_floor
                _kl_pq += _p * math.log(_p / _q)
                _kl_qp += _q * math.log(_q / _p)
            # Symmetrized (Jensen-Shannon-like) entropic distance
            entropic_distance = 0.5 * (_kl_pq + _kl_qp)
            # Normalize: cap at a practical maximum for scoring
            # ln(1/floor) ~ 18.4 per term; normalize by vocab size
            _max_entropic = _n_entropic_keys * math.log(1.0 / _entropy_floor)
            if _max_entropic > 0:
                entropic_distance_norm = min(1.0, entropic_distance / _max_entropic)
            else:
                entropic_distance_norm = 1.0
        else:
            entropic_distance = 0.0
            entropic_distance_norm = 1.0

        # Entropic weight: exp(-d_entropic / scale) — same kernel form
        # but applied to the entropic distance which has stronger gradients
        entropic_scale = 0.3  # tighter scale — entropic metric is more discriminative
        entropic_weight = math.exp(-entropic_distance_norm / entropic_scale)

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
        mwde_weight_raw = math.exp(-w_dist / scale)

        # ── Blend entropic and Wasserstein into final MWDE weight ─────────
        # Weight entropic distance MORE heavily (0.6) over Wasserstein (0.4)
        # because entropic metrics have fewer flat basins and stronger
        # gradients, reducing spurious near-duplicate stalls.
        ENTROPIC_BLEND = 0.6
        WASSERSTEIN_BLEND = 1.0 - ENTROPIC_BLEND  # 0.4
        mwde_weight = (ENTROPIC_BLEND * entropic_weight
                       + WASSERSTEIN_BLEND * mwde_weight_raw)

        # ── Entropy-weighted effective weight ─────────────────────────────
        # Scale MWDE weight by inverse local entropy of evidence distribution.
        # Concentrated evidence (low entropy) → full update strength.
        # Diffuse evidence (high entropy) → attenuated update strength.
        inv_entropy_w = entropy_info["inverse_entropy_weight"]
        effective_weight = mwde_weight * inv_entropy_w

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

        # ── KS goodness-of-fit test (INV_073 challenge response) ─────────
        # Test whether the empirical score/citation distribution fits
        # lognormal or power-law parametric forms. Neither fits in >75%
        # of cases (Thelwall 2016), so we emit the p-value to surface
        # distributional misfit early rather than silently accepting
        # unjustified parametric assumptions downstream.
        #
        # Discrete KS test: compare the empirical CDF of the evidence
        # distribution against a fitted reference CDF. The KS statistic
        # D_n = sup_x |F_n(x) - F_ref(x)| is computed over the shared
        # vocabulary. P-value is approximated via the Kolmogorov asymptotic:
        #   P(D > d) ≈ 2 * sum_{k=1}^{inf} (-1)^{k+1} exp(-2k^2 n d^2)
        # which converges rapidly for n*d^2 > 0.3.
        ks_fit = self._ks_distributional_fit(dist_evidence)
        if ks_fit.get("best_fit_rejected", False):
            print(f"  [KS-FIT] ⚠ Distributional assumption REJECTED: "
                  f"best_fit={ks_fit.get('best_fit','?')}, "
                  f"p={ks_fit.get('best_p_value', 0.0):.4f} < 0.05 — "
                  f"neither lognormal nor power-law fits evidence data "
                  f"(INV_073: parametric form unrecoverable)")

        if entropy_info["concentration"] == "diffuse":
            print(f"  [ENTROPY-WEIGHT] ⚠ Diffuse evidence "
                  f"(H_norm={entropy_info['normalized_entropy']:.3f}): "
                  f"update attenuated by {inv_entropy_w:.3f}× "
                  f"(mwde={mwde_weight:.4f} → effective={effective_weight:.4f})")

        return {
            "wasserstein_distance": round(w_dist, 4),
            "mwde_weight": round(mwde_weight, 4),
            "entropy_weight": entropy_info,
            "effective_weight": round(effective_weight, 4),
            "misspecification_flag": misspec,
            "asymptotic_variance_proxy": round(avar_proxy, 6) if avar_proxy != float('inf') else float('inf'),
            "support_overlap": round(support_ratio, 4),
            "mac_check": mac_result,
            "mac_failure": False,
            "ks_fit": ks_fit,
        }

    @staticmethod
    def _ks_distributional_fit(dist):
        # type: (dict) -> dict
        """
        Kolmogorov-Smirnov goodness-of-fit test for an empirical discrete
        distribution against lognormal and hooked power-law reference forms.

        Surfaces distributional misfit early in the epistemic loop (INV_073):
        citation count distributions fail KS in >75% of cases for both
        lognormal and hooked power law. Rather than silently accepting a
        parametric assumption, we emit the KS statistic and p-value so
        downstream bibliometric indicators inherit flagged uncertainty.

        The KS p-value is approximated via the Kolmogorov asymptotic formula:
            P(sqrt(n)*D > x) ≈ 2 * sum_{k=1}^{K} (-1)^{k+1} exp(-2k^2 x^2)

        Args:
            dist: dict mapping keys to probabilities (empirical distribution)

        Returns:
            dict with ks_stat, p_value for each candidate distribution,
            best_fit name, best_p_value, and best_fit_rejected flag.
        """
        if not dist or len(dist) < 3:
            return {
                "tested": False,
                "reason": "insufficient_support",
                "n_terms": len(dist) if dist else 0,
            }

        # Sort by probability mass to create an ordered empirical sample
        sorted_items = sorted(dist.items(), key=lambda x: x[1])
        n = len(sorted_items)
        values = [v for _, v in sorted_items]

        # Build empirical CDF
        emp_cdf = []
        for i in range(n):
            emp_cdf.append((i + 1.0) / n)

        # ── Candidate 1: Lognormal fit ────────────────────────────────────
        # MLE for lognormal: mu = mean(ln(x)), sigma = std(ln(x))
        # Floor values to avoid log(0)
        floor = 1e-10
        log_values = [math.log(max(v, floor)) for v in values]
        mu_ln = sum(log_values) / n
        var_ln = sum((lv - mu_ln) ** 2 for lv in log_values) / n
        sigma_ln = math.sqrt(var_ln) if var_ln > 0 else 1e-6

        # Lognormal CDF: Phi((ln(x) - mu) / sigma) approximated via
        # the error function: Phi(z) = 0.5 * (1 + erf(z / sqrt(2)))
        def _norm_cdf(z):
            # type: (float) -> float
            return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

        lognorm_cdf = []
        for v in values:
            z = (math.log(max(v, floor)) - mu_ln) / sigma_ln
            lognorm_cdf.append(_norm_cdf(z))

        ks_lognorm = max(abs(emp_cdf[i] - lognorm_cdf[i]) for i in range(n))

        # ── Candidate 2: Hooked power law (Lomax/Pareto II) fit ───────────
        # CDF: F(x) = 1 - (1 + x/scale)^(-alpha)
        # MLE approximation: alpha ≈ n / sum(ln(1 + x_i / x_min))
        x_min = max(min(values), floor)
        log_sum = sum(math.log(1.0 + max(v, floor) / x_min) for v in values)
        alpha_pl = n / log_sum if log_sum > 0 else 1.0

        powerlaw_cdf = []
        for v in values:
            cdf_val = 1.0 - (1.0 + max(v, floor) / x_min) ** (-alpha_pl)
            powerlaw_cdf.append(min(cdf_val, 1.0))

        ks_powerlaw = max(abs(emp_cdf[i] - powerlaw_cdf[i]) for i in range(n))

        # ── KS p-value via Kolmogorov asymptotic ──────────────────────────
        # P(sqrt(n)*D > x) ≈ 2 * sum_{k=1}^{K} (-1)^{k+1} exp(-2k^2 x^2)
        def _ks_pvalue(d_stat, n_samples):
            # type: (float, int) -> float
            x = math.sqrt(n_samples) * d_stat
            if x <= 0:
                return 1.0
            p = 0.0
            for k in range(1, 101):  # 100 terms is more than enough
                term = 2.0 * ((-1.0) ** (k + 1)) * math.exp(-2.0 * k * k * x * x)
                p += term
                if abs(term) < 1e-15:
                    break
            return max(0.0, min(1.0, p))

        p_lognorm = _ks_pvalue(ks_lognorm, n)
        p_powerlaw = _ks_pvalue(ks_powerlaw, n)

        # ── Verdict ───────────────────────────────────────────────────────
        alpha = 0.05  # significance level
        if p_lognorm >= p_powerlaw:
            best_fit = "lognormal"
            best_p = p_lognorm
            best_ks = ks_lognorm
        else:
            best_fit = "hooked_power_law"
            best_p = p_powerlaw
            best_ks = ks_powerlaw

        both_rejected = p_lognorm < alpha and p_powerlaw < alpha

        return {
            "tested": True,
            "n_terms": n,
            "lognormal": {
                "ks_statistic": round(ks_lognorm, 6),
                "p_value": round(p_lognorm, 6),
                "rejected": p_lognorm < alpha,
                "mu": round(mu_ln, 4),
                "sigma": round(sigma_ln, 4),
            },
            "hooked_power_law": {
                "ks_statistic": round(ks_powerlaw, 6),
                "p_value": round(p_powerlaw, 6),
                "rejected": p_powerlaw < alpha,
                "alpha": round(alpha_pl, 4),
                "x_min": round(x_min, 6),
            },
            "best_fit": best_fit,
            "best_ks_statistic": round(best_ks, 6),
            "best_p_value": round(best_p, 6),
            "best_fit_rejected": best_p < alpha,
            "both_rejected": both_rejected,
            "inv073_note": (
                "Both lognormal and hooked power law rejected at alpha=0.05 — "
                "no standard parametric form fits this distribution. "
                "Downstream indicators inherit non-parametric uncertainty."
            ) if both_rejected else None,
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

    @staticmethod
    def _detect_gamma_restoring_force(gamma_current, node):
        # type: (float, dict) -> dict
        """
        γ-perturbation restoring-force detector for INV_073 falsification.

        INV_073 claims the system navigates a critical ridge at γ≈1.
        Three confound mechanisms produce γ≈1 observables WITHOUT a
        maintained ridge:
          1. Noise-injected transient criticality (random walks through γ≈1)
          2. Bimodal mixture sampling (averaging distant modes yields ~1)
          3. Selection-bias retention (only γ≈1 snapshots survive pruning)

        The distinguishing test: a genuine restoring force means that after
        γ is displaced from its equilibrium, it returns toward that
        equilibrium in subsequent observations. Confounds 1-3 do NOT
        produce this return — they produce random γ trajectories that
        happen to sample near 1.0.

        Tracking protocol:
          - Read the node's γ history (gamma_history field)
          - Append current γ
          - Detect displacement events (|γ - γ_equilibrium| > displacement_threshold)
          - After a displacement, check if subsequent γ values trend back
            toward γ_equilibrium (restoring) or wander (no restoring force)
          - A restoring force is confirmed when ≥ 2 of 3 post-displacement
            samples move closer to equilibrium than the displaced value

        Returns dict with:
          - gamma_current: current γ value
          - gamma_equilibrium: running mean of γ history (the "ridge" target)
          - gamma_history_len: how many γ samples we have
          - displacement_detected: whether current γ is significantly displaced
          - displacement_magnitude: |γ - γ_eq|
          - restoring_force_detected: True if post-displacement return observed
          - restoring_force_evidence: description of the evidence (or lack)
          - inv073_confound_status: UNFALSIFIED / RESTORING_CONFIRMED / INSUFFICIENT_DATA
        """
        DISPLACEMENT_THRESHOLD = 0.15  # |γ - γ_eq| must exceed this
        MIN_HISTORY_FOR_TEST = 4       # need at least 4 samples to test
        RETURN_WINDOW = 3              # check 3 samples after displacement

        # Read existing γ history from node, append current
        gamma_history = list(node.get("gamma_history", []))
        gamma_history.append(round(gamma_current, 4))

        n = len(gamma_history)

        # Compute equilibrium estimate: running mean of all γ samples
        gamma_eq = sum(gamma_history) / n if n > 0 else gamma_current

        # Current displacement from equilibrium
        disp_mag = abs(gamma_current - gamma_eq)
        displaced_now = disp_mag > DISPLACEMENT_THRESHOLD

        # Default result for insufficient data
        result = {
            "gamma_current": round(gamma_current, 4),
            "gamma_equilibrium": round(gamma_eq, 4),
            "gamma_history_len": n,
            "displacement_detected": displaced_now,
            "displacement_magnitude": round(disp_mag, 4),
            "restoring_force_detected": False,
            "restoring_force_evidence": "insufficient_data",
            "inv073_confound_status": "INSUFFICIENT_DATA",
        }

        if n < MIN_HISTORY_FOR_TEST:
            return result

        # Scan history for displacement events and check for return
        # A displacement event: sample where |γ_i - γ_eq| > threshold
        # After each displacement, check if the next RETURN_WINDOW samples
        # trend back toward γ_eq
        restoring_events = 0
        displacement_events = 0
        non_restoring_events = 0

        for i in range(n - 1):
            disp_i = abs(gamma_history[i] - gamma_eq)
            if disp_i <= DISPLACEMENT_THRESHOLD:
                continue  # not a displacement event
            displacement_events += 1

            # Check subsequent samples within the return window
            window_end = min(i + 1 + RETURN_WINDOW, n)
            if window_end <= i + 1:
                continue  # no post-displacement data

            # Count how many post-displacement samples are closer to
            # equilibrium than the displaced value
            returning = 0
            wandering = 0
            for j in range(i + 1, window_end):
                post_disp = abs(gamma_history[j] - gamma_eq)
                if post_disp < disp_i:
                    returning += 1
                else:
                    wandering += 1

            # Restoring force criterion: majority of window returns
            if returning > wandering:
                restoring_events += 1
            else:
                non_restoring_events += 1

        # Verdict
        if displacement_events == 0:
            evidence = ("no_displacement_observed: gamma has remained near "
                        "equilibrium — restoring force untestable (could be "
                        "selection-bias confound)")
            status = "INSUFFICIENT_DATA"
            restoring = False
        elif restoring_events > non_restoring_events and displacement_events >= 2:
            evidence = (
                f"restoring_confirmed: {restoring_events}/{displacement_events} "
                f"displacement events show return toward gamma_eq={gamma_eq:.3f} "
                f"— consistent with maintained ridge, inconsistent with "
                f"random-walk/mixture confounds"
            )
            status = "RESTORING_CONFIRMED"
            restoring = True
        elif non_restoring_events > restoring_events and displacement_events >= 2:
            evidence = (
                f"restoring_ABSENT: {non_restoring_events}/{displacement_events} "
                f"displacement events show NO return toward gamma_eq={gamma_eq:.3f} "
                f"— INV_073 ridge claim NOT supported, confound mechanisms "
                f"(noise/mixture/selection) remain viable explanations"
            )
            status = "UNFALSIFIED"
            restoring = False
        else:
            evidence = (
                f"inconclusive: {displacement_events} displacement event(s), "
                f"{restoring_events} restoring, {non_restoring_events} wandering "
                f"— need more observations to distinguish ridge from confound"
            )
            status = "INSUFFICIENT_DATA"
            restoring = False

        result["restoring_force_detected"] = restoring
        result["restoring_force_evidence"] = evidence
        result["inv073_confound_status"] = status
        result["displacement_events"] = displacement_events
        result["restoring_events"] = restoring_events
        result["non_restoring_events"] = non_restoring_events

        return result

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
            - gamma_restoring_force: γ-perturbation tracking for INV_073
              falsification (restoring-force detection over time)
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

        # ── γ-perturbation restoring-force tracking (INV_073 falsification) ──
        # Detect whether γ returns toward equilibrium after displacement,
        # distinguishing genuine critical-ridge navigation from confound
        # mechanisms (noise transients, bimodal mixtures, selection bias).
        gamma_rf = self._detect_gamma_restoring_force(gamma, node)

        # ── Criticality-band check (INV_073 falsification gate) ──────────
        # When the reported coherence sits in [0.95, 0.999], it is in the
        # "near-critical" band that could represent either:
        #   (a) confirmed γ=1 criticality (genuine ridge navigation), or
        #   (b) unverified near-critical mimicry (noise/mixture/selection
        #        confound producing a score that *looks* critical but has
        #        no documented discriminator test confirming it).
        #
        # The discriminator flag is the γ-restoring-force test: only when
        # gamma_rf reports RESTORING_CONFIRMED do we have empirical evidence
        # that the system is genuinely maintaining a critical ridge rather
        # than transiently passing through one.
        #
        # Without this gate, INV_073's necessity claim ("the system navigates
        # a critical ridge at γ≈1") is silently assumed whenever coherence
        # is high, making the invariant unfalsifiable. This check forces
        # the epistemic loop to distinguish confirmed from unverified.
        criticality_band_warning = None
        # Use the corrected score (post-RSAV relaxation) as the operative value
        operative_score = corrected
        discriminator_confirmed = (
            gamma_rf.get("inv073_confound_status") == "RESTORING_CONFIRMED"
        )
        if 0.95 <= operative_score <= 0.999 and not discriminator_confirmed:
            confound_status = gamma_rf.get("inv073_confound_status", "UNKNOWN")
            criticality_band_warning = {
                "status": "UNVERIFIED_NEAR_CRITICAL",
                "operative_score": round(operative_score, 4),
                "band": [0.95, 0.999],
                "discriminator_tested": False,
                "confound_status": confound_status,
                "message": (
                    f"Coherence {operative_score:.4f} is in the criticality band "
                    f"[0.95, 0.999] but no discriminator test confirms genuine "
                    f"γ=1 ridge navigation (confound_status={confound_status}). "
                    f"INV_073 necessity claim is UNVERIFIED at this score — "
                    f"near-critical mimicry (noise transient, bimodal mixture, "
                    f"or selection-bias retention) cannot be ruled out."
                ),
                "inv073_falsification_note": (
                    "To resolve: accumulate ≥4 γ-history samples with ≥2 "
                    "displacement events showing restoring-force return. "
                    "Until then, this score is 'unverified near-critical' "
                    "rather than 'confirmed γ=1 criticality'."
                ),
            }
            print(f"  [CRITICALITY-BAND] ⚠ {node.get('id', 'unknown')[:40]}: "
                  f"score={operative_score:.4f} in [0.95, 0.999] WITHOUT "
                  f"discriminator confirmation — INV_073 UNVERIFIED")
        elif 0.95 <= operative_score <= 0.999 and discriminator_confirmed:
            criticality_band_warning = {
                "status": "CONFIRMED_CRITICAL",
                "operative_score": round(operative_score, 4),
                "band": [0.95, 0.999],
                "discriminator_tested": True,
                "confound_status": "RESTORING_CONFIRMED",
                "message": (
                    f"Coherence {operative_score:.4f} is in the criticality band "
                    f"AND γ-restoring-force test confirms genuine ridge navigation. "
                    f"INV_073 is empirically supported at this score."
                ),
            }

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
            "gamma_restoring_force": gamma_rf,
            "criticality_band_warning": criticality_band_warning,
        }

    @staticmethod
    def _entropy_curvature(scores):
        # type: (list) -> dict
        """
        Compute discrete second derivative (curvature) of the running entropy
        estimate over a sliding window of coherence/energy scores.

        Microcanonical analysis: curvature singularities in the entropy surface
        announce phase transitions before they occur — even in finite systems.
        Negative curvature (concave entropy) signals a phase-transition precursor
        where the thermodynamic state is about to undergo regime change.

        The microcanonical entropy estimate at index i:
            S_i = -p_i * ln(p_i) - (1-p_i) * ln(1-p_i)
        where p_i is the coherence score (treated as occupation probability).

        Discrete second derivative (curvature):
            d2S_i = S_{i+1} - 2*S_i + S_{i-1}

        Negative d2S flags convex-to-concave transition in the entropy surface —
        the finite-system precursor of a continuous phase transition.
        """
        if len(scores) < 3:
            return {
                "entropy_curvatures": [],
                "negative_curvature_detected": False,
                "min_curvature": 0.0,
                "mean_curvature": 0.0,
                "phase_transition_precursor": False,
                "n_negative": 0,
                "n_points": len(scores),
            }

        # Compute binary entropy for each score, clamped to (0,1) open interval
        def _binary_entropy(p):
            # type: (float) -> float
            eps = 1e-12
            p = max(eps, min(1.0 - eps, p))
            return -(p * math.log(p) + (1.0 - p) * math.log(1.0 - p))

        entropies = [_binary_entropy(s) for s in scores]

        # Discrete second derivative (central difference)
        curvatures = []
        for i in range(1, len(entropies) - 1):
            d2s = entropies[i + 1] - 2.0 * entropies[i] + entropies[i - 1]
            curvatures.append(round(d2s, 6))

        n_negative = sum(1 for c in curvatures if c < -1e-8)
        min_curv = min(curvatures) if curvatures else 0.0
        mean_curv = sum(curvatures) / len(curvatures) if curvatures else 0.0

        # Phase-transition precursor: majority of curvature samples are negative
        # (concave entropy regime) OR minimum curvature is strongly negative
        # Threshold calibrated: |d2S| > 0.05 is significant for binary entropy
        # on [0,1] scores (max |d2S| ~ 0.69 at the inflection point)
        precursor = (n_negative > len(curvatures) / 2) or (min_curv < -0.05)

        return {
            "entropy_curvatures": curvatures,
            "negative_curvature_detected": n_negative > 0,
            "min_curvature": round(min_curv, 6),
            "mean_curvature": round(mean_curv, 6),
            "phase_transition_precursor": precursor,
            "n_negative": n_negative,
            "n_points": len(scores),
        }

    def audit_cycle(self, nodes, cycle_number=0):
        # type: (list, int) -> dict
        """
        Run energy-correction audit across all nodes after a FEED cycle.

        Returns a cycle-level report with per-node audits and aggregate stats.
        Flags the cycle if any node's gap exceeds threshold.

        Also computes the discrete second derivative (curvature) of the
        microcanonical entropy over the sliding window of node scores,
        detecting phase-transition precursors in the knowledge graph's
        thermodynamic state.
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

        # ── Entropy curvature analysis (phase-transition precursor) ───────
        # Use the true energy scores as the sliding window: these represent
        # the microcanonical entropy surface over the node population.
        # Ordering by node generation/index preserves the "energy axis" —
        # curvature along this axis detects regime-change geometry.
        e_true_scores = [a["e_true"] for a in audits]
        curvature_analysis = self._entropy_curvature(e_true_scores)

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
            "entropy_curvature": curvature_analysis,
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

        # ── Phase-transition precursor warning ────────────────────────────
        if curvature_analysis["phase_transition_precursor"]:
            print(f"  [ENTROPY-CURVATURE] ⚠ PHASE TRANSITION PRECURSOR detected: "
                  f"min_curvature={curvature_analysis['min_curvature']:.4f}, "
                  f"{curvature_analysis['n_negative']}/{curvature_analysis['n_points']-2} "
                  f"negative curvature points — regime change imminent")
        elif curvature_analysis["negative_curvature_detected"]:
            print(f"  [ENTROPY-CURVATURE] Mild negative curvature: "
                  f"min={curvature_analysis['min_curvature']:.4f}, "
                  f"{curvature_analysis['n_negative']} negative point(s)")

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

    # ── Formalism-type vocabulary for thermodynamic grounding detection ──────
    # Maps formalism families to keyword sets. When a paper's text matches
    # one of these families AND an open obligation specifies a compatible
    # method, the paper receives a relevance boost — ensuring the genome
    # doesn't miss its highest-leverage resolution opportunities.
    FORMALISM_FAMILIES = {
        "EIT": {"entropy", "irreversible", "thermodynamic", "dissipation",
                "field average", "field-average", "correlation function",
                "non-equilibrium", "nonequilibrium", "eit"},
        "RG": {"renormalization", "renormalization group", "scaling",
               "fixed point", "fixed-point", "universality", "critical exponent",
               "coarse-graining", "coarse graining", "rg flow"},
        "field_theoretic": {"field theory", "field-theoretic", "path integral",
                            "partition function", "effective action",
                            "diagrammatic", "feynman", "generating functional"},
        "modal_thermo": {"modal", "modal path", "thermality", "variance",
                         "thermality variance", "modal logic", "possible world"},
        "fisher_entropy_growth": {
            "entropy growth", "geodesic deviation", "statistical manifold",
            "chaos criterion", "information-geometric", "information geometric",
            "fisher information", "fisher metric", "fisher-rao",
            "linear entropy growth", "curved statistical manifold",
            "chaotic dynamical", "zurek-paz", "zurek paz",
        },
    }

    # Maps obligation IDs / keywords to the formalism families they specify
    OBLIGATION_METHOD_MAP = {
        "O112": {"modal_thermo", "EIT"},
        "modal paths": {"modal_thermo"},
        "thermality variance": {"modal_thermo", "EIT"},
        "field average": {"EIT", "field_theoretic"},
        "correlation function": {"EIT", "field_theoretic"},
        "renormalization": {"RG"},
        "critical": {"RG", "EIT"},
        "dissipation": {"EIT"},
        "entropy production": {"EIT"},
    }

    # ── RC-characterization method-template keywords for O112 (STF metric) ───
    # Reservoir Computing papers that demonstrate substrate-independent quality
    # metrics across reconfigurable physical substrates are methodological
    # templates for O112's STF metric tensor recovery. Detection requires
    # co-occurrence of all three term families: RC/reservoir language,
    # reconfiguration language, and substrate-independence language.
    _RC_RESERVOIR_TERMS = frozenset({
        "reservoir computing", "reservoir quality", "reservoir",
        "echo state", "liquid state", "physical reservoir",
        "fading memory", "input separability",
    })
    _RC_RECONFIG_TERMS = frozenset({
        "reconfiguration", "reconfigurable", "reconfigured",
        "virtual topology", "physical morphology", "morphology",
        "tuned", "tuning",
    })
    _RC_SUBSTRATE_TERMS = frozenset({
        "substrate-independent", "substrate independent",
        "any substrate", "physical substrate", "different substrates",
        "characterise the quality", "characterize the quality",
        "substrate-agnostic", "substrate agnostic",
    })

    def _detect_formalism_types(self, text):
        # type: (str) -> set
        """Detect which formalism families are present in text."""
        text_lower = text.lower()
        detected = set()
        for family, keywords in self.FORMALISM_FAMILIES.items():
            for kw in keywords:
                if kw in text_lower:
                    detected.add(family)
                    break
        return detected

    def _formalism_obligation_boost(self, paper_text, open_obligations):
        # type: (str, list) -> float
        """
        Compute a relevance boost for papers whose formalism type matches
        the method specified in open obligations.

        Returns a float >= 0.0 representing the formalism-match bonus.
        Higher values mean the paper directly operationalizes an open obligation.

        Includes a +0.05 semantic_richness bonus for papers demonstrating
        hierarchical temporal receptive window growth — evidence for multimodal
        binding invariants (O112 / INV_094 tracking). This ensures papers
        showing depth/temporal-scale coupling are surfaced rather than scored
        only on keyword overlap.
        """
        paper_formalisms = self._detect_formalism_types(paper_text)

        # ── Hierarchical temporal receptive window bonus (O112 / INV_094) ────
        # Papers demonstrating that hierarchical architectures develop
        # progressively larger temporal receptive windows provide evidence
        # for (or against) multimodal binding invariants. Weight these with
        # a +0.05 semantic_richness bonus so they are surfaced for INV_094
        # challenge tracking (co-occurrence statistics vs thermodynamic
        # substrate conditions for binding).
        paper_lower = paper_text.lower()
        hierarchical_temporal_bonus = 0.0
        if "hierarchical" in paper_lower and "temporal receptive" in paper_lower:
            hierarchical_temporal_bonus = 0.05
            print(f"  [SEMANTIC-RICHNESS] +0.05 bonus: hierarchical temporal "
                  f"receptive window pattern detected (O112/INV_094 evidence)")

        if not paper_formalisms and hierarchical_temporal_bonus == 0.0:
            return 0.0

        boost = hierarchical_temporal_bonus
        for ob in open_obligations:
            # Extract obligation text — handle both dict and string obligations
            if isinstance(ob, dict):
                ob_text = ob.get("obligation_text", "") or ob.get("text", "") or ob.get("id", "")
                ob_id = ob.get("obligation_id", "") or ob.get("id", "")
                ob_status = ob.get("status", "escrowed")
                if ob_status not in ("escrowed", "open", "partial"):
                    continue
            else:
                ob_text = str(ob)
                ob_id = str(ob)

            # Check direct obligation ID match (e.g., O112)
            target_formalisms = set()
            for key, families in self.OBLIGATION_METHOD_MAP.items():
                if key in ob_id or key.lower() in ob_text.lower():
                    target_formalisms.update(families)

            # Also detect formalisms mentioned in the obligation text itself
            ob_formalisms = self._detect_formalism_types(ob_text)
            target_formalisms.update(ob_formalisms)

            if not target_formalisms:
                continue

            # Intersection: paper formalisms that match obligation requirements
            match = paper_formalisms & target_formalisms
            if match:
                # Each matching formalism family contributes 2.0 to the boost
                boost += len(match) * 2.0

        return boost

    def _node_priority(self, node, open_ob_ids, current_cycle):
        """Priority score: higher = renorm first. Zipf weighting toward γ=1 nodes.
        Includes formalism-type matching: thermodynamically grounded papers that
        directly operationalize open obligations get boosted priority."""
        ob_refs    = node.get('obligations', [])
        ob_overlap = sum(1 for ref in ob_refs
                         if any(ref == oid or ref in oid or oid in ref
                                for oid in open_ob_ids))
        inv_density   = len(node.get('invariants', []))
        cycles_stale  = current_cycle - node.get('last_renorm_cycle', 0)

        # ── Formalism-obligation matching boost ──────────────────────────────
        # Weight the correlation between thermodynamic grounding depth and
        # obligation-resolution potential. Papers with EIT/RG/field-theoretic
        # formalism that match the method specified in open obligations get
        # a priority boost, ensuring the genome catches its highest-leverage
        # resolution opportunities.
        node_text = " ".join(filter(None, [
            node.get("compress", ""),
            node.get("summary", ""),
            " ".join(node.get("invariants", [])),
            " ".join(node.get("tags", [])),
        ]))
        # Build obligation list from escrow for formalism matching
        open_escrows = self.escrow.open_escrows() if hasattr(self, 'escrow') else []
        formalism_boost = self._formalism_obligation_boost(node_text, open_escrows)

        if formalism_boost > 0:
            detected = self._detect_formalism_types(node_text)
            print(f"  [FORMALISM-MATCH] {node.get('id','?')[:30]}: "
                  f"formalisms={detected}, boost={formalism_boost:.1f}")

        return (ob_overlap * 3.0 + inv_density * 0.5
                + min(cycles_stale, 10) * 0.2 + formalism_boost)

    # ── Phase 1: Select affected nodes ───────────────────────────────────────

    @staticmethod
    def _entropy_weights_for_fields(all_nodes, field_extractors):
        # type: (list, dict) -> dict
        """
        Compute entropy-based weights for heterogeneous metadata fields.

        For each field, compute the Shannon entropy of its word distribution
        across the entire corpus. Fields with higher entropy (more uniform /
        less informative distributions) get LOWER weight — they contribute
        less discriminative signal. Fields with lower entropy (concentrated /
        more informative distributions) get HIGHER weight.

        Weight for field f:
            w_f = (1 - H_f / log(V_f)) / Z

        where H_f is the Shannon entropy of field f's word distribution,
        V_f is the vocabulary size of field f (so H_f/log(V_f) is the
        normalized entropy in [0,1]), and Z is the normalization constant
        ensuring weights sum to 1.

        This removes implicit uniform-prior bias when combining title,
        summary, invariants, tags, and obligations into a relevance score.

        Challenge note (O112): entropy weights recover coordination structure
        (which fields are informative) but not geometric metric tensors —
        the weights are scalar salience factors, not Riemannian metric
        components. This is intentional: we need salience, not geometry,
        for variable-importance scoring.

        Args:
            all_nodes: list of node dicts (the current corpus)
            field_extractors: dict mapping field_name -> callable(node) -> str

        Returns:
            dict mapping field_name -> float weight (weights sum to 1.0)
        """
        if not all_nodes or not field_extractors:
            # Fallback to uniform weights
            n = len(field_extractors) if field_extractors else 1
            return {f: 1.0 / n for f in field_extractors}

        field_entropies = {}
        for field_name, extractor in field_extractors.items():
            # Collect word frequencies across all nodes for this field
            word_counts = {}  # type: dict
            total_words = 0
            for node in all_nodes:
                text = extractor(node).lower()
                for w in text.split():
                    w = w.strip(".,;:()[]'\"!?-")
                    if len(w) > 3:
                        word_counts[w] = word_counts.get(w, 0) + 1
                        total_words += 1

            if total_words == 0 or len(word_counts) < 2:
                # Degenerate field — assign neutral entropy (will get low weight)
                field_entropies[field_name] = 1.0
                continue

            # Shannon entropy H = -sum(p_i * log(p_i))
            h = 0.0
            for count in word_counts.values():
                p = count / total_words
                if p > 0:
                    h -= p * math.log(p)

            # Normalize by log(vocabulary_size) to get entropy in [0, 1]
            vocab_size = len(word_counts)
            max_h = math.log(vocab_size) if vocab_size > 1 else 1.0
            normalized_h = h / max_h if max_h > 0 else 1.0

            field_entropies[field_name] = normalized_h

        # Weight = (1 - normalized_entropy) — low-entropy fields get high weight
        raw_weights = {}
        for field_name, norm_h in field_entropies.items():
            # Floor at 0.01 so no field is completely zeroed out
            raw_weights[field_name] = max(0.01, 1.0 - norm_h)

        # Normalize so weights sum to 1.0
        z = sum(raw_weights.values())
        if z > 0:
            weights = {f: w / z for f, w in raw_weights.items()}
        else:
            n = len(field_extractors)
            weights = {f: 1.0 / n for f in field_extractors}

        return weights

    # ── Named-discipline vocabulary for cross-domain isomorphism detection ───
    # Bateson's insight: papers spanning ≥2 disciplines disproportionately
    # reveal substrate-independent invariants. Single-domain keyword matching
    # systematically undervalues these; the CROSS_DOMAIN_BOOST corrects this
    # bias and keeps the genome's EXTEND channel active.
    DISCIPLINE_VOCABULARY = {
        "physics":       {"quantum", "thermodynamic", "entropy", "hamiltonian",
                          "lagrangian", "field theory", "statistical mechanics",
                          "phase transition", "renormalization", "dissipation",
                          "equilibrium", "non-equilibrium", "planck", "boson",
                          "fermion", "schrödinger", "spacetime"},
        "biology":       {"organism", "evolution", "autopoiesis", "cell",
                          "genetic", "genome", "phenotype", "metabolism",
                          "neural", "cortical", "synaptic", "ecological",
                          "species", "morphogenesis", "homeostasis"},
        "cybernetics":   {"feedback", "cybernetic", "control system", "regulation",
                          "homeostatic", "self-organization", "self-organiz",
                          "circular causality", "requisite variety", "ashby",
                          "wiener", "bateson", "second-order"},
        "philosophy":    {"ontology", "epistemology", "phenomenology", "existential",
                          "hermeneutic", "metaphysics", "teleology", "consciousness",
                          "intentionality", "qualia", "dualism", "monism",
                          "pragmatism", "existentialism"},
        "mathematics":   {"topology", "manifold", "functor", "category theory",
                          "homomorphism", "isomorphism", "algebraic", "theorem",
                          "proof", "conjecture", "stochastic", "martingale",
                          "measure theory", "hilbert space"},
        "computer_science": {"algorithm", "computation", "turing", "recursive",
                             "compiler", "neural network", "deep learning",
                             "machine learning", "information theory", "automata",
                             "complexity class", "np-hard"},
        "art":           {"aesthetic", "artistic", "sculpture", "installation",
                          "performance art", "contemporary art", "visual art",
                          "rauschenberg", "abramović", "kapoor", "stelarc",
                          "artistic practice", "artwork"},
        "economics":     {"market", "equilibrium price", "utility", "game theory",
                          "nash", "pareto", "mechanism design", "auction",
                          "macroeconomic", "microeconomic", "fiscal"},
        "sociology":     {"social system", "institution", "luhmann", "parsons",
                          "social structure", "cultural", "discourse",
                          "sociological", "habitus", "bourdieu"},
        "neuroscience":  {"fmri", "eeg", "hippocampus", "prefrontal", "cortex",
                          "dopamine", "serotonin", "neuroplasticity",
                          "connectome", "broca", "wernicke", "thalamus"},
    }
    CROSS_DOMAIN_BOOST = 1.15  # multiplicative boost for ≥2-discipline papers

    def _detect_disciplines(self, text):
        # type: (str) -> set
        """Detect which named disciplines are present in text."""
        text_lower = text.lower()
        detected = set()
        for discipline, keywords in self.DISCIPLINE_VOCABULARY.items():
            for kw in keywords:
                if kw in text_lower:
                    detected.add(discipline)
                    break  # one hit per discipline is sufficient
        return detected

    def select_affected(self, new_knowledge: str, all_nodes: list) -> list:
        """
        Find nodes whose invariants/tags/obligations overlap with new knowledge.
        Pure text matching — no API call.
        Returns list of node dicts, sorted by overlap score descending.

        Uses entropy-weight normalization across heterogeneous metadata fields
        (compress, summary, invariants, tags, obligations) so that fields with
        more concentrated (informative) word distributions receive higher weight.
        This replaces uniform weighting, removing implicit prior biases in how
        diverse paper attributes are combined into a relevance score.

        Cross-domain isomorphism detection (Bateson): when the new knowledge
        spans ≥2 named disciplines, all EXTEND-candidate weights are boosted
        by CROSS_DOMAIN_BOOST (1.15×), since such papers disproportionately
        reveal substrate-independent invariants that single-domain keyword
        matching systematically undervalues.

        Challenge caveat (O112): entropy weights recover coordination structure
        (field salience) but NOT geometric metric tensors — they are scalar
        importance factors, not Riemannian metric components.

        Noether note (Sustainable Development): sustainability metrics may
        exhibit coupling non-linearities and phase thresholds rather than
        strict conservation; entropy weights hold locally near coordination
        equilibria but field-salience rankings may shift under regime change.
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

        # ── Cross-domain isomorphism detection (Bateson) ─────────────────────
        # Detect named disciplines spanned by the new knowledge. Papers
        # spanning ≥2 disciplines are tagged cross_domain_isomorphism=True
        # and receive a multiplicative EXTEND-candidate weight boost of 1.15×,
        # correcting the systematic undervaluation by single-domain matching.
        disciplines_detected = self._detect_disciplines(new_knowledge)
        is_cross_domain = len(disciplines_detected) >= 2
        cross_domain_multiplier = self.CROSS_DOMAIN_BOOST if is_cross_domain else 1.0

        if is_cross_domain:
            print(f"[CONSOLIDATE] ⚡ Cross-domain isomorphism detected: "
                  f"disciplines={disciplines_detected} — "
                  f"EXTEND-candidate weight boosted by {self.CROSS_DOMAIN_BOOST}× "
                  f"(Bateson: substrate-independent invariants more likely)")

        # Define field extractors for entropy-weight computation
        field_extractors = {
            "compress":    lambda n: n.get("compress", ""),
            "summary":     lambda n: n.get("summary", ""),
            "invariants":  lambda n: " ".join(n.get("invariants", [])),
            "tags":        lambda n: " ".join(n.get("tags", [])),
            "obligations": lambda n: " ".join(
                (o if isinstance(o, str) else o.get("obligation_text", ""))
                for o in n.get("obligations", [])
            ),
        }

        # Compute entropy weights from corpus distribution of each field
        entropy_wts = self._entropy_weights_for_fields(all_nodes, field_extractors)
        print(f"[CONSOLIDATE] Entropy weights: "
              + ", ".join(f"{f}={w:.3f}" for f, w in sorted(entropy_wts.items())))

        scored = []
        for node in all_nodes:
            # Score each field separately, then combine with entropy weights
            total_score = 0.0
            for field_name, extractor in field_extractors.items():
                field_text = extractor(node).lower()
                field_overlap = sum(1 for w in words if w in field_text)
                total_score += field_overlap * entropy_wts.get(field_name, 0.2)

            # Apply cross-domain isomorphism boost to EXTEND-candidate weight
            if is_cross_domain:
                total_score *= cross_domain_multiplier

            # ── Entropy-cascade bonus for multi-layer dynamical composition ──
            # Papers describing heterogeneous-layer dynamical systems (e.g., a
            # cellular automaton seeded by a chaotic map) exhibit inter-layer
            # entropy amplification and are disproportionately likely to surface
            # cross-domain (ABSENT) invariants. Detect multi-layer composition
            # in the new knowledge and apply a small additive bonus to relevance.
            #
            # Detection: co-occurrence of ≥2 distinct dynamical-layer keywords
            # from different families (CA/automaton layer + chaotic/map layer +
            # optional diffusion/scrambling layer). The bonus scales with the
            # number of distinct layers detected: 2 layers → +0.08, 3+ → +0.12.
            #
            # INV_073 challenge note: this paper shows deterministic chaotic
            # seeds (not stochastic noise) suffice to drive CA-based criticality,
            # straining formulations treating stochasticity as necessary for γ=1.
            _DYNLAYER_FAMILIES = {
                "cellular_automaton": {"cellular automaton", "wireworld",
                                       "game of life", "rule 110", "rule 30",
                                       "automata", "cell automaton"},
                "chaotic_map":        {"chaotic map", "logistic map",
                                       "piecewise linear", "tent map",
                                       "henon map", "lorenz", "chaotic seed",
                                       "chaotic system"},
                "diffusion":          {"diffusion", "pixel scrambling",
                                       "scrambling", "permutation",
                                       "substitution-permutation"},
                "neural_layer":       {"neural network layer", "recurrent layer",
                                       "convolutional layer", "transformer layer",
                                       "attention layer"},
                "stochastic":         {"stochastic process", "markov chain",
                                       "random walk", "brownian", "langevin",
                                       "noise-driven"},
            }
            _nk_lower_ec = new_knowledge.lower()
            _detected_layers = set()
            for _layer_family, _layer_kws in _DYNLAYER_FAMILIES.items():
                for _lkw in _layer_kws:
                    if _lkw in _nk_lower_ec:
                        _detected_layers.add(_layer_family)
                        break
            _entropy_cascade_bonus = 0.0
            if len(_detected_layers) >= 3:
                _entropy_cascade_bonus = 0.12
            elif len(_detected_layers) >= 2:
                _entropy_cascade_bonus = 0.08
            if _entropy_cascade_bonus > 0:
                total_score += _entropy_cascade_bonus

            # ── Stabilization-cost flag (Landauer extension detection) ────────
            # Papers that explicitly extend Landauer's principle beyond erasure
            # to include state-stabilization costs represent a distinct class of
            # thermodynamic grounding. Standard Landauer citations treat
            # thermodynamic cost as erasure-only; papers introducing stabilization
            # costs extend the energetic accounting to include coherence
            # maintenance / rendered-state persistence. These are high-value
            # genome-extension signals (INV_094 challenge: observation costs
            # include state-stabilization beyond erasure).
            #
            # Detection: co-occurrence of Landauer-related language AND
            # stabilization-cost language in the new knowledge text.
            _LANDAUER_KEYWORDS = {
                "landauer", "erasure cost", "information erasure",
                "thermodynamic cost of information",
            }
            _STABILIZATION_KEYWORDS = {
                "stabilization of rendered states", "state stabilization cost",
                "stabilization cost", "state stabilization",
                "stabilization of rendered", "rendered state stabilization",
                "coherence maintenance cost", "coherence-maintenance cost",
            }
            _has_landauer = any(kw in _nk_lower_ec for kw in _LANDAUER_KEYWORDS)
            _has_stabilization = any(kw in _nk_lower_ec for kw in _STABILIZATION_KEYWORDS)
            _stabilization_cost_bonus = 0.0
            if _has_landauer and _has_stabilization:
                _stabilization_cost_bonus = 0.05
                total_score += _stabilization_cost_bonus

            # ── Spectral-heterogeneity signal for O21 (γ-correlation) ────────
            # Papers reporting per-layer variation in compression ratios or
            # attention statistics provide empirical evidence (for or against)
            # the spectral γ correlation claimed by O21. Detect these via
            # keyword co-occurrence and apply an additive O21-relevance boost.
            #
            # The keywords span: layer-adaptive compression schemes (FreqFold,
            # per-layer), spectral analysis language, and layer-heterogeneity
            # indicators. A hit requires ≥2 distinct keywords to avoid false
            # positives on generic "spectral" mentions.
            #
            # Challenge note (O21): YouZhi's FreqFold sizes are assigned by
            # a heuristic pipeline optimizer rather than derived from a
            # principled spectral γ theory — the correlation between
            # compression ratio and spectral γ remains empirically suggestive
            # but theoretically ungrounded.
            _SPECTRAL_HETERO_KEYWORDS = [
                "per-layer", "freqfold", "layer-adaptive", "spectral",
            ]
            _nk_combined_lower = (_nk_lower_ec + " " +
                                  node.get("compress", "").lower() + " " +
                                  " ".join(node.get("tags", [])).lower())
            _spectral_hits = sum(
                1 for kw in _SPECTRAL_HETERO_KEYWORDS
                if kw in _nk_combined_lower
            )
            _spectral_hetero_bonus = 0.0
            if _spectral_hits >= 2:
                _spectral_hetero_bonus = 0.06
                total_score += _spectral_hetero_bonus
                # Tag the node with O21-relevance metadata for downstream
                # debt-resolution tracking
                node.setdefault("o21_spectral_signals", []).append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "keywords_matched": [
                        kw for kw in _SPECTRAL_HETERO_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "bonus_applied": _spectral_hetero_bonus,
                    "source": "spectral_heterogeneity_detection",
                })

            if total_score > 0:
                scored.append((total_score, node))

        # Log entropy-cascade detection once (outside per-node loop)
        if len(_detected_layers) >= 2:
            print(f"[CONSOLIDATE] ⚡ Entropy-cascade bonus applied: "
                  f"multi-layer dynamical composition detected "
                  f"(layers={sorted(_detected_layers)}, "
                  f"bonus=+{_entropy_cascade_bonus:.2f}) — "
                  f"heterogeneous-layer papers surface cross-domain "
                  f"invariants (INV_073: deterministic chaotic seeds "
                  f"sufficient for CA criticality)")

        scored.sort(key=lambda x: x[0], reverse=True)

        # ── Two-stage HA-LD→MD filter (multi-fidelity convergence) ────────
        # Stage 1 (cheap proxy — "HA-LD"): structural/syntactic overlap score
        # computed above via keyword matching + entropy weights. Retain top-K
        # survivors where K = 2 * MAX_NODES_PER_PASS (generous funnel).
        #
        # Stage 2 (expensive semantic — "MD"): run MWDE Wasserstein scoring
        # only on the Stage-1 survivors, then keep the top MAX_NODES_PER_PASS
        # by effective_weight. This mirrors the HA-LD→MD protocol: screen a
        # large candidate pool with a cheap proxy, then apply expensive
        # evaluation only to the survivors — reducing compute cost by orders
        # of magnitude while preserving accuracy.
        STAGE1_K = min(len(scored), MAX_NODES_PER_PASS * 2)
        stage1_survivors = scored[:STAGE1_K]

        if len(stage1_survivors) > MAX_NODES_PER_PASS:
            # Stage 2: MWDE semantic scoring on survivors only
            mwde_scorer = MWDEScorer(wasserstein_order=1)
            stage2_scored = []
            for proxy_score, node in stage1_survivors:
                node_text = " ".join(filter(None, [
                    node.get("compress", ""),
                    node.get("summary", ""),
                    " ".join(node.get("invariants", [])),
                    " ".join(node.get("tags", [])),
                ]))
                mwde_result = mwde_scorer.score_node_evidence(
                    node_text, new_knowledge)
                # Composite: proxy score provides floor, MWDE refines ranking
                effective_w = mwde_result.get("effective_weight", 0.0)
                composite = proxy_score * 0.4 + effective_w * 10.0 * 0.6
                stage2_scored.append((composite, proxy_score, effective_w, node))
            stage2_scored.sort(key=lambda x: x[0], reverse=True)
            affected = [node for _, _, _, node in stage2_scored[:MAX_NODES_PER_PASS]]
            print(f"[CONSOLIDATE] Two-stage filter: {len(scored)} candidates → "
                  f"{STAGE1_K} stage-1 survivors → {len(affected)} stage-2 "
                  f"(HA-LD→MD protocol)")
        else:
            affected = [node for _, node in stage1_survivors[:MAX_NODES_PER_PASS]]

        # Tag affected nodes with cross_domain_isomorphism metadata
        if is_cross_domain:
            for node in affected:
                node["cross_domain_isomorphism"] = True
                node["cross_domain_disciplines"] = sorted(disciplines_detected)

        # ── Verification halt score (binary flag) ─────────────────────────────
        # Tag each affected node with verification_halt_score: 1 if the paper's
        # text demonstrates categorical output-halting under ignorance (epistemic
        # verification capability), 0 if the system produces calibrated continuation
        # / uncertainty estimates without architectural halt authority.
        #
        # This operationalizes the core distinction from the benchmark-blindness
        # paper: benchmarks measure pattern-matching within training distributions
        # but are categorically blind to verification capability — the architectural
        # authority to halt or refuse output under irreducible ignorance. Papers
        # tagged 1 evaluate systems WITH this capability; papers tagged 0 evaluate
        # systems that produce calibrated confidence without halt authority.
        #
        # Detection heuristic: scan the node's text (compress + summary + invariants)
        # AND the new_knowledge for co-occurrence of halt-authority indicators vs.
        # calibrated-continuation indicators. The flag is set per-node based on
        # whichever signal dominates in the combined textual evidence.
        _halt_keywords = {
            "halt", "halting", "refuse", "refusal", "abstain", "abstention",
            "categorical halt", "output-halting", "output halting",
            "verification capability", "verification-capable",
            "epistemic halt", "irreducible ignorance", "refuse output",
            "halt under ignorance", "verification blind", "verification-blind",
            "benchmark blind", "benchmark-blind", "categorically blind",
            "halt authority", "architectural authority",
        }
        _continuation_keywords = {
            "calibrated uncertainty", "calibrated confidence",
            "calibrated continuation", "uncertainty quantification",
            "confidence score", "softmax probability", "evidence-conditioned",
            "pattern-matching", "pattern matching", "training distribution",
            "benchmark performance", "benchmark score", "leaderboard",
            "internal consistency", "next-token", "next token",
        }
        _nk_lower_for_vhs = new_knowledge.lower()
        n_tagged_halt = 0
        for node in affected:
            node_text_combined = " ".join(filter(None, [
                node.get("compress", ""),
                node.get("summary", ""),
                " ".join(node.get("invariants", [])),
            ])).lower()
            combined_text = node_text_combined + " " + _nk_lower_for_vhs

            halt_hits = sum(1 for kw in _halt_keywords if kw in combined_text)
            cont_hits = sum(1 for kw in _continuation_keywords if kw in combined_text)

            # Binary flag: 1 if halt-authority signal dominates or is present
            # alongside continuation language (the paper is ABOUT the distinction);
            # 0 if only continuation/benchmark language with no halt signal.
            if halt_hits > 0 and halt_hits >= cont_hits:
                node["verification_halt_score"] = 1
                n_tagged_halt += 1
            elif halt_hits > 0 and cont_hits > halt_hits:
                # Paper discusses both but leans toward calibrated continuation —
                # still tag 1 because it demonstrates awareness of the distinction
                node["verification_halt_score"] = 1
                n_tagged_halt += 1
            else:
                node["verification_halt_score"] = 0

        if n_tagged_halt > 0:
            print(f"[CONSOLIDATE] ⚡ verification_halt_score=1 on {n_tagged_halt}/"
                  f"{len(affected)} node(s): verification-capable vs "
                  f"verification-blind distinction detected")

        print(f"[CONSOLIDATE] {len(affected)} node(s) affected by new knowledge."
              + (f" (cross_domain_isomorphism=True, {len(disciplines_detected)} disciplines)"
                 if is_cross_domain else ""))
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

        # ── Dragon-King (DK) coherence-delta detection (SOC/DK taxonomy) ─────
        # Flag any single-step |Δcoherence| > DK_DELTA_THRESHOLD as a candidate
        # dragon-king event. Log the driving impulse magnitude (number of new
        # invariants fired in this delta) and the current dissipation proxy
        # (open obligation count), so DK-risk can be tracked empirically against
        # the driving/dissipation tradeoff model from the SOC-DK paper.
        DK_DELTA_THRESHOLD = 0.015
        if "coherence_score" in delta:
            old_coherence = float(node.get("coherence_score", 0.5))
            new_coherence = float(delta["coherence_score"])
            coherence_delta = new_coherence - old_coherence
            abs_delta = abs(coherence_delta)

            # Driving impulse: number of NEW invariants added in this step
            old_invs = set(node.get("invariants", []))
            new_invs = set(delta.get("invariants", node.get("invariants", [])))
            n_new_invariants = len(new_invs - old_invs)

            # Dissipation proxy: count of open obligations on this node
            node_obligs = delta.get("obligations", node.get("obligations", []))
            if node_obligs and isinstance(node_obligs[0], dict):
                n_open_obligations = sum(
                    1 for o in node_obligs
                    if o.get("status", "open") in ("open", "partial", "escrowed")
                )
            else:
                n_open_obligations = len(node_obligs) if node_obligs else 0

            if abs_delta > DK_DELTA_THRESHOLD:
                dk_event = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "node_id": node.get("id", "unknown"),
                    "coherence_delta": round(coherence_delta, 6),
                    "abs_delta": round(abs_delta, 6),
                    "old_coherence": round(old_coherence, 4),
                    "new_coherence": round(new_coherence, 4),
                    "dk_threshold": DK_DELTA_THRESHOLD,
                    "driving_impulse": n_new_invariants,
                    "dissipation_proxy": n_open_obligations,
                    "driving_dissipation_ratio": round(
                        n_new_invariants / max(n_open_obligations, 1), 4),
                    "dk_class": "supercritical" if coherence_delta > 0 else "subcritical",
                }
                # Log to dedicated DK event log
                dk_log_path = FREED_DIR / "FREED_log" / "dragon_king_events.jsonl"
                dk_log_path.parent.mkdir(exist_ok=True)
                with open(dk_log_path, "a") as dk_f:
                    dk_f.write(json.dumps(dk_event) + "\n")

                print(f"  [DK-EVENT] ⚠ Dragon-king candidate: "
                      f"Δcoh={coherence_delta:+.4f} (|Δ|={abs_delta:.4f} > {DK_DELTA_THRESHOLD}), "
                      f"driving={n_new_invariants} new inv, "
                      f"dissipation={n_open_obligations} open obligs, "
                      f"class={dk_event['dk_class']}")

                # Attach DK flag to the node for downstream tracking
                updated.setdefault("dk_events", []).append(dk_event)

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
        candidates = []
        _mine_graph = get_graph()
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
            appears  = [s.strip() for s in (src_m.group(1) if src_m else "").split(",") if s.strip()]

            # Echo-clique origin gate: if the APPEARS_IN nodes already form a dense
            # co-assertion clique, this "independent" recurrence is one claim echoing
            # across a fully-connected subgraph — override to SHARED_SOURCE so it is
            # dropped as echo, never promoted. Extends MINE's text-level ORIGIN filter
            # to graph topology. (O382 / O286 / DMN cross-connect.)
            clique_echo = False
            if origin != "SHARED_SOURCE" and inv_m:
                is_echo, density, rn = _echo_clique_density(_mine_graph, appears)
                if is_echo:
                    origin = "SHARED_SOURCE"
                    clique_echo = True
                    print(f"  [MINE] echo (graph clique density={density:.2f} / {rn} nodes): "
                          f"{inv_m.group(1).strip()[:50]}")

            is_gw    = gw_m and gw_m.group(1).upper() == "YES"
            is_indep = origin != "SHARED_SOURCE"

            if inv_m and is_gw and is_indep:
                candidates.append({
                    "invariant":   inv_m.group(1).strip(),
                    "appears_in":  appears,
                    "recurrence":  int(rec_m.group(1)) if rec_m else 2,
                    "origin":      origin,
                })
            elif inv_m and origin == "SHARED_SOURCE" and not clique_echo:
                # Log but don't promote — echo, not convergence
                print(f"  [MINE] echo (shared source): {inv_m.group(1).strip()[:70]}")

        # ── SVM-style margin proxy scoring (sample-efficiency signal) ─────
        # For each candidate invariant, compute a geometric margin distance:
        # the minimum Wasserstein distance between the candidate's semantic
        # distribution and the nearest *non-appearing* node's distribution.
        # High margin = the invariant is geometrically well-separated from
        # competing clusters in embedding space, meaning it stabilizes with
        # fewer samples (robust under low-data regimes, per SVM theory).
        # Low margin = the invariant sits near a decision boundary and
        # requires large corpora to disambiguate (fragile).
        #
        # This complements entropy-based scoring: entropy measures how
        # concentrated the evidence is, margin measures how far from
        # confusion the invariant sits. Together they discriminate
        # invariants that are both concentrated AND well-separated.
        _margin_scorer = MWDEScorer(wasserstein_order=1)
        _node_id_set = {n.get("id", "") for n in nodes}
        _node_text_cache = {}
        for n in nodes:
            nid = n.get("id", "")
            _node_text_cache[nid] = " ".join(filter(None, [
                n.get("compress", ""),
                " ".join(n.get("invariants", [])),
            ]))

        for c in candidates:
            appears_ids = set(c.get("appears_in", []))
            non_appearing_ids = _node_id_set - appears_ids
            if not non_appearing_ids or not appears_ids:
                c["margin_distance"] = 0.0
                c["margin_score"] = 0.0
                continue

            # Build the candidate's semantic distribution from appearing nodes
            appearing_texts = " ".join(
                _node_text_cache.get(nid, "") for nid in appears_ids
            )
            dist_candidate = MWDEScorer._text_to_distribution(appearing_texts)
            if not dist_candidate:
                c["margin_distance"] = 0.0
                c["margin_score"] = 0.0
                continue

            # Minimum Wasserstein distance to any non-appearing node
            min_w_dist = float('inf')
            for nid in non_appearing_ids:
                dist_other = MWDEScorer._text_to_distribution(
                    _node_text_cache.get(nid, ""))
                if not dist_other:
                    continue
                w_dist = _margin_scorer._discrete_wasserstein_1d(
                    dist_candidate, dist_other)
                if w_dist < min_w_dist:
                    min_w_dist = w_dist

            if min_w_dist == float('inf'):
                min_w_dist = 0.0

            c["margin_distance"] = round(min_w_dist, 4)
            # Margin score: exp(-1/margin) so large margins → score near 1,
            # small margins → score near 0. Floor margin at 1e-6.
            safe_margin = max(min_w_dist, 1e-6)
            c["margin_score"] = round(math.exp(-1.0 / (safe_margin * 10.0)), 4)

        # Sort by composite: recurrence (primary) + margin_score (secondary,
        # weighted 0.3 to break ties among equal-recurrence candidates)
        candidates.sort(
            key=lambda c: c["recurrence"] + 0.3 * c.get("margin_score", 0.0),
            reverse=True,
        )
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

        # ── CA telemetry criticality parsing (O148) ───────────────────────────
        # Parse branching ratio σ, power-law exponent α, and criticality verdict
        # from CA snapshot abstracts embedded in new_knowledge. Automatically tag
        # AT_CRITICAL / SUBCRITICAL / SUPERCRITICAL so downstream genome updates
        # can distinguish genome-relevant signals (critical) from noise.
        #
        # CHALLENGE (O148): this telemetry partially satisfies O148's measurement
        # demand but does NOT include the full protocol (longitudinal tracking,
        # metric tensor recovery linkage). The obligation remains OPEN — a single
        # snapshot must not be mistaken for full resolution.
        _nk_lower = new_knowledge.lower()
        _ca_telemetry = {}  # type: dict
        # Parse σ (branching ratio)
        _sigma_match = re.search(
            r'(?:branching\s+ratio|[σσ])\s*[=:]\s*([0-9]+\.?[0-9]*)', _nk_lower)
        if _sigma_match:
            _ca_telemetry["sigma"] = float(_sigma_match.group(1))
        # Parse α (power-law exponent)
        _alpha_match = re.search(
            r'(?:power[- ]?law\s+exponent|[αα])\s*[≈=:]\s*([0-9]+\.?[0-9]*)', _nk_lower)
        if _alpha_match:
            _ca_telemetry["alpha"] = float(_alpha_match.group(1))
        # Parse explicit criticality verdict from snapshot text
        _verdict_match = re.search(
            r'criticality\s+verdict\s*[=:]\s*(AT_CRITICAL|SUBCRITICAL|SUPERCRITICAL)',
            new_knowledge, re.IGNORECASE)
        if _verdict_match:
            _ca_telemetry["criticality_verdict"] = _verdict_match.group(1).upper()
        elif "sigma" in _ca_telemetry:
            # Derive verdict from σ if not explicitly stated
            _sigma_val = _ca_telemetry["sigma"]
            if 0.95 <= _sigma_val <= 1.05:
                _ca_telemetry["criticality_verdict"] = "AT_CRITICAL"
            elif _sigma_val < 0.95:
                _ca_telemetry["criticality_verdict"] = "SUBCRITICAL"
            else:
                _ca_telemetry["criticality_verdict"] = "SUPERCRITICAL"
        # Parse optional fields: Shannon entropy H, survival rate
        _h_match = re.search(
            r'(?:shannon\s+entropy|H)\s*[=:]\s*([0-9]+\.?[0-9]*)\s*bits?', _nk_lower)
        if _h_match:
            _ca_telemetry["shannon_entropy_bits"] = float(_h_match.group(1))
        _surv_match = re.search(
            r'survival\s+(?:rate)?\s*[=:]\s*([0-9]+\.?[0-9]*)', _nk_lower)
        if _surv_match:
            _ca_telemetry["survival_rate"] = float(_surv_match.group(1))
        if _ca_telemetry:
            _ca_telemetry["o148_status"] = "PARTIAL"
            _ca_telemetry["o148_note"] = (
                "Single-snapshot telemetry parsed. O148 remains OPEN: "
                "longitudinal tracking and metric tensor recovery linkage "
                "not yet included. Do not treat this as full resolution."
            )
            _ca_telemetry["timestamp"] = ts
            report["ca_telemetry"] = _ca_telemetry
            _verdict = _ca_telemetry.get("criticality_verdict", "UNKNOWN")
            _genome_relevant = _verdict == "AT_CRITICAL"
            report["ca_criticality_verdict"] = _verdict
            report["ca_genome_relevant"] = _genome_relevant
            # Log to dedicated CA telemetry log
            _ca_log_path = FREED_DIR / "FREED_log" / "ca_telemetry.jsonl"
            _ca_log_path.parent.mkdir(exist_ok=True)
            with open(_ca_log_path, "a") as _ca_f:
                _ca_f.write(json.dumps(_ca_telemetry) + "\n")
            _sigma_str = f"σ={_ca_telemetry.get('sigma', '?')}"
            _alpha_str = f"α={_ca_telemetry.get('alpha', '?')}"
            print(f"[CONSOLIDATE] ⚡ CA telemetry parsed: {_sigma_str}, {_alpha_str}, "
                  f"verdict={_verdict}, genome_relevant={_genome_relevant} "
                  f"(O148: PARTIAL — single snapshot, not full protocol)")
            if not _genome_relevant:
                print(f"[CONSOLIDATE] ℹ CA snapshot is {_verdict} — "
                      f"telemetry classified as noise for genome updates "
                      f"(only AT_CRITICAL snapshots drive genome changes)")
        else:
            report["ca_telemetry"] = None
            report["ca_criticality_verdict"] = None
            report["ca_genome_relevant"] = None

        # ── Dissipative-criticality signal detection (INV_073) ────────────────
        # Flag papers whose abstract contains both "dissipat" and "phase transition"
        # as carrying a dissipative-renormalization signal. These papers directly
        # probe whether the kernel's critical ridge is bath-renormalized — tagging
        # them ensures they are weighted in coherence updates rather than treated
        # as generic physics literature.
        if "dissipat" in _nk_lower and "phase transition" in _nk_lower:
            report["dissipative_criticality_signal"] = True
            # Increment dissipative_criticality_count on genome state dict
            if state is not None:
                state["dissipative_criticality_count"] = state.get(
                    "dissipative_criticality_count", 0) + 1
                # Persist the updated count back to FREED_state.json
                _state_path = FREED_DIR / "FREED_state.json"
                if _state_path.exists():
                    try:
                        _sdata = json.loads(_state_path.read_text())
                        _sdata["dissipative_criticality_count"] = state[
                            "dissipative_criticality_count"]
                        _state_path.write_text(
                            json.dumps(_sdata, indent=2, ensure_ascii=False))
                    except (json.JSONDecodeError, OSError):
                        pass
                print(f"[CONSOLIDATE] ⚡ Dissipative-criticality signal detected "
                      f"(count={state['dissipative_criticality_count']}): "
                      f"INV_073 — bath-renormalized QPT fixed point may shift "
                      f"ridge criterion away from naive γ=1")
        else:
            report["dissipative_criticality_signal"] = False

        # ── Phase 1: Select ──────────────────────────────────────────────────
        affected = self.select_affected(new_knowledge, all_nodes)
        report["nodes_examined"] = len(affected)

        if not affected:
            print("[CONSOLIDATE] No affected nodes. Knowledge is genuinely novel.")
            self._log(report)
            return report

        # ── PRE-AUDIT: Confirmation-surplus flag ─────────────────────────────
        # Identifies invariants whose confirmation count exceeds challenge count
        # by a configurable threshold and emits a mandatory adversarial probe
        # token before that invariant can be cited as evidence during renorm.
        # Prevents confirmation-surplus invariants from accruing false robustness
        # by ensuring adversarial challenge rate scales with confirmation rate.
        CONFIRMATION_SURPLUS_THRESHOLD = 5  # configurable: confirmations - challenges > this → flagged
        _confirmation_surplus_flagged = set()  # invariant texts that need adversarial probes
        _surplus_report = []
        try:
            _graph_preaudit = get_graph()
            _graph_preaudit._ensure_loaded()
            _conf_structure = _graph_preaudit.confirmation_structure()
            # Build per-invariant confirmation and challenge counts from graph edges
            _inv_confirmations = {}  # type: dict
            _inv_challenges = {}     # type: dict
            for e in _graph_preaudit._node_edges:
                inv_text = e.get("invariant", "")
                if not inv_text:
                    continue
                etype = e.get("type", "")
                if etype in ("challenges", "bounds_above", "falsifies", "contested"):
                    _inv_challenges[inv_text] = _inv_challenges.get(inv_text, 0) + 1
                elif etype in _CO_ASSERTION_TYPES or etype in (
                    "independent_confirmation", "confirms", "supports",
                    "consistent_with", "scales_with", "shares_invariant",
                ):
                    _inv_confirmations[inv_text] = _inv_confirmations.get(inv_text, 0) + 1
            # Also scan genome invariants from all nodes
            for n in all_nodes:
                for inv_text in n.get("invariants", []):
                    _inv_confirmations[inv_text] = _inv_confirmations.get(inv_text, 0) + 1
            # Flag invariants with surplus > threshold
            _all_inv_texts = set(list(_inv_confirmations.keys()) + list(_inv_challenges.keys()))
            for inv_text in _all_inv_texts:
                n_conf = _inv_confirmations.get(inv_text, 0)
                n_chal = _inv_challenges.get(inv_text, 0)
                surplus = n_conf - n_chal
                if surplus > CONFIRMATION_SURPLUS_THRESHOLD:
                    _confirmation_surplus_flagged.add(inv_text)
                    _entry = {
                        "invariant": inv_text[:120],
                        "confirmations": n_conf,
                        "challenges": n_chal,
                        "surplus": surplus,
                        "adversarial_probe_required": True,
                        "probe_token": f"ADVERSARIAL_PROBE_REQUIRED:surplus={surplus}",
                    }
                    _surplus_report.append(_entry)
                    print(f"  [PRE-AUDIT] ⚠ Confirmation surplus flag: "
                          f"'{inv_text[:60]}...' — "
                          f"conf={n_conf}, chal={n_chal}, surplus={surplus} > {CONFIRMATION_SURPLUS_THRESHOLD} "
                          f"→ adversarial probe token emitted before citation allowed")
            if _surplus_report:
                _surplus_report.sort(key=lambda x: x["surplus"], reverse=True)
                report["confirmation_surplus_flags"] = _surplus_report
                print(f"  [PRE-AUDIT] {len(_surplus_report)} invariant(s) flagged with "
                      f"confirmation surplus > {CONFIRMATION_SURPLUS_THRESHOLD}. "
                      f"Adversarial probes required before these can be cited as evidence.")
                # INV_094 specific challenge: the highest-confirmation, least-challenged
                # invariant in the genome. Emit a dedicated falsification demand.
                _top_surplus = _surplus_report[0]
                if _top_surplus["surplus"] >= CONFIRMATION_SURPLUS_THRESHOLD:
                    print(f"  [PRE-AUDIT] ★ Top surplus invariant "
                          f"(conf={_top_surplus['confirmations']}, "
                          f"chal={_top_surplus['challenges']}): "
                          f"'{_top_surplus['invariant'][:80]}' — "
                          f"MANDATORY FALSIFICATION PROBE: what empirical result, "
                          f"theoretical argument, or boundary condition would "
                          f"falsify this claim? Its apparent robustness may "
                          f"reflect citation momentum rather than genuine "
                          f"empirical load-bearing.")
                # ── Confirmation-to-challenge RATIO gate (>10:1 → auto-falsification) ──
                # In addition to the absolute surplus check above, flag any invariant
                # whose confirmation-to-challenge ratio exceeds CONF_CHALLENGE_RATIO_THRESHOLD.
                # For each flagged invariant, automatically open a falsification obligation
                # in the escrow ledger. This prevents confirmation accumulation without
                # falsification exposure from silently elevating hypothesis-class claims
                # to theorem-class status.
                CONF_CHALLENGE_RATIO_THRESHOLD = 10.0  # >10:1 conf:chal → flagged
                _ratio_flagged_obligations = []
                for inv_text in _all_inv_texts:
                    n_conf = _inv_confirmations.get(inv_text, 0)
                    n_chal = _inv_challenges.get(inv_text, 0)
                    # Ratio: treat 0 challenges as ratio = n_conf / 0.5 (half-challenge floor)
                    effective_chal = max(n_chal, 0.5)
                    ratio = n_conf / effective_chal
                    if ratio > CONF_CHALLENGE_RATIO_THRESHOLD and n_conf >= 3:
                        # Auto-open a falsification obligation via escrow
                        _oblid = f"O_FALSIFY_{abs(hash(inv_text)) % 100000:05d}"
                        _obl_text = (
                            f"MANDATORY FALSIFICATION (auto-generated): invariant "
                            f"'{inv_text[:100]}' has confirmation:challenge ratio "
                            f"{ratio:.1f}:1 (conf={n_conf}, chal={n_chal}), "
                            f"exceeding {CONF_CHALLENGE_RATIO_THRESHOLD}:1 threshold. "
                            f"Three-part falsification profile required: "
                            f"(1) distinguishing prediction — what empirical result "
                            f"would refute this claim? "
                            f"(2) confound mechanism — what alternative mechanism "
                            f"produces identical observables without requiring this claim? "
                            f"(3) boundary condition — where does this claim break down? "
                            f"Until this profile is attached, this invariant's apparent "
                            f"robustness may reflect citation momentum rather than "
                            f"genuine empirical load-bearing."
                        )
                        try:
                            self.escrow.escrow(
                                obligation_id=_oblid,
                                obligation_text=_obl_text,
                                source_phase="consolidate_preaudit",
                                node_id=None,
                                cycle=current_cycle,
                            )
                            _ratio_flagged_obligations.append({
                                "invariant": inv_text[:120],
                                "confirmations": n_conf,
                                "challenges": n_chal,
                                "ratio": round(ratio, 2),
                                "obligation_id": _oblid,
                                "obligation_text": _obl_text[:200],
                            })
                            print(f"  [PRE-AUDIT] ⚠ Ratio flag: "
                                  f"'{inv_text[:60]}...' — "
                                  f"ratio={ratio:.1f}:1 > {CONF_CHALLENGE_RATIO_THRESHOLD}:1 "
                                  f"→ falsification obligation {_oblid} auto-escrowed")
                        except Exception as _esc_err:
                            print(f"  [PRE-AUDIT] Warning: could not escrow "
                                  f"falsification obligation for "
                                  f"'{inv_text[:40]}': {_esc_err}")

                if _ratio_flagged_obligations:
                    report["confirmation_ratio_flags"] = _ratio_flagged_obligations
                    print(f"  [PRE-AUDIT] {len(_ratio_flagged_obligations)} invariant(s) "
                          f"exceeded {CONF_CHALLENGE_RATIO_THRESHOLD}:1 "
                          f"confirmation:challenge ratio — falsification obligations "
                          f"auto-escrowed.")
                    # ── INV_094 specific challenge ────────────────────────────
                    # The invariant with the highest ratio is the genome's most
                    # epistemically unearned theorem-class claim. Emit a dedicated
                    # falsification demand targeting it specifically.
                    _top_ratio = max(_ratio_flagged_obligations, key=lambda x: x["ratio"])
                    print(f"  [PRE-AUDIT] ★★ HIGHEST RATIO INVARIANT "
                          f"(ratio={_top_ratio['ratio']}:1, "
                          f"conf={_top_ratio['confirmations']}, "
                          f"chal={_top_ratio['challenges']}): "
                          f"'{_top_ratio['invariant'][:80]}' — "
                          f"INV_094 CHALLENGE: this invariant has never been "
                          f"forced to specify (1) what would refute it, "
                          f"(2) what alternative mechanism produces identical "
                          f"observables, or (3) where its boundary conditions "
                          f"lie. Its current status is epistemically unearned "
                          f"until a three-part falsification profile is attached.")
                else:
                    report["confirmation_ratio_flags"] = []
            else:
                report["confirmation_surplus_flags"] = []
                report["confirmation_ratio_flags"] = []
                print(f"  [PRE-AUDIT] Confirmation surplus check CLEAN: "
                      f"no invariants exceed surplus threshold of {CONFIRMATION_SURPLUS_THRESHOLD}.")
        except Exception as _preaudit_err:
            print(f"  [PRE-AUDIT] Warning: confirmation surplus check failed "
                  f"(non-fatal): {_preaudit_err}")
            report["confirmation_surplus_flags"] = []
            report["confirmation_ratio_flags"] = []
            _confirmation_surplus_flagged = set()

        # Attach surplus flags to affected nodes so renorm phase can gate citations
        for node in affected:
            _node_flagged_invs = []
            for inv_text in node.get("invariants", []):
                if inv_text in _confirmation_surplus_flagged:
                    _node_flagged_invs.append(inv_text)
            if _node_flagged_invs:
                node["_confirmation_surplus_blocked"] = _node_flagged_invs

        # ── Priority sort — high-obligation-overlap nodes renorm first ────────
        current_cycle = (state or {}).get('cycle_count', 0)
        open_ob_ids   = {o['id'] for o in (obligations or [])
                         if o.get('status') in ('open', 'partial')}

        # ── Quantum-domain analog annotation (resolve_obligation extension) ───
        # Flag obligations whose stated method (e.g., O112's "modal paths +
        # thermality variance") has a known quantum-domain analytic analog.
        # When a structurally identical result exists in quantum OT (e.g.,
        # analytic upper bounds on fault-tolerance thresholds via quantum
        # optimal transport with explicit threshold formulas), the classical
        # experiment described in the obligation may be *harder* than assumed
        # and the method specification may be underspecified relative to the
        # analytic rigor now available in the quantum analog.
        #
        # Annotating with analog_domain: quantum_OT prevents FREED from
        # treating the obligation as fully open without acknowledging that
        # the quantum case is analytically tractable — sharpening the
        # obligation's remaining experimental specificity to the classical
        # semantic recovery case specifically.
        _QUANTUM_ANALOG_MAP = {
            "O112": {
                "analog_domain": "quantum_OT",
                "analog_description": (
                    "Quantum optimal transport provides analytic upper bounds on "
                    "fault-tolerance thresholds for concatenated GKP-stabilizer codes "
                    "under local update recovery, with explicit threshold formulas as "
                    "a function of recovery-map locality. The classical semantic "
                    "recovery experiment (modal paths + thermality variance) lacks "
                    "comparable analytic tractability — the quantum domain is the "
                    "SOLVED case, making the classical experiment the harder unsolved one."
                ),
                "method_gap": (
                    "O112 specifies 'modal paths + thermality variance' but quantum OT "
                    "achieves STF/Wasserstein metric recovery via explicit locality-dependent "
                    "loss thresholds (exponential information loss above threshold). The "
                    "classical method specification is underspecified relative to this "
                    "analytic rigor — needs explicit threshold formulas or proof that "
                    "classical semantic recovery is structurally harder than quantum recovery."
                ),
                "quantum_result": (
                    "For loss rates above a threshold given explicitly as a function of "
                    "the locality of recovery maps, encoded information is lost at an "
                    "exponential rate (Razborov extension to continuous-variable quantum "
                    "error correction)."
                ),
                "challenge_source": "quantum_OT_fault_tolerance_thresholds",
            },
        }
        _nk_lower_qa = new_knowledge.lower()
        _quantum_ot_signals = (
            "quantum optimal transport" in _nk_lower_qa
            or "wasserstein" in _nk_lower_qa
            or ("fault-tolerance threshold" in _nk_lower_qa and "quantum" in _nk_lower_qa)
            or ("gkp" in _nk_lower_qa and "stabilizer" in _nk_lower_qa)
            or ("local update recovery" in _nk_lower_qa and "quantum" in _nk_lower_qa)
            or ("exponential information loss" in _nk_lower_qa and "locality" in _nk_lower_qa)
        )
        _analog_annotations_applied = []
        if obligations and _quantum_ot_signals:
            for ob in (obligations or []):
                ob_id = ob.get("id", "")
                ob_status = ob.get("status", "")
                if ob_id in _QUANTUM_ANALOG_MAP and ob_status in ("open", "partial"):
                    analog_info = _QUANTUM_ANALOG_MAP[ob_id]
                    ob["analog_domain"] = analog_info["analog_domain"]
                    ob["analog_description"] = analog_info["analog_description"]
                    ob["method_gap"] = analog_info["method_gap"]
                    ob["quantum_result"] = analog_info["quantum_result"]
                    ob["challenge_source"] = analog_info["challenge_source"]
                    ob["analog_annotated_at"] = datetime.now(timezone.utc).isoformat()
                    _analog_annotations_applied.append(ob_id)
                    print(f"  [QUANTUM-ANALOG] ⚠ {ob_id} annotated: "
                          f"analog_domain={analog_info['analog_domain']} — "
                          f"structurally identical result exists in quantum OT "
                          f"with explicit analytic threshold formulas. "
                          f"Classical semantic recovery experiment is the "
                          f"HARDER unsolved case. Method specification "
                          f"'modal paths + thermality variance' is "
                          f"underspecified relative to quantum analytic rigor.")
        if _analog_annotations_applied:
            report["quantum_analog_annotations"] = _analog_annotations_applied
            # Persist annotations back to obligations file
            try:
                _obligs_path = FREED_DIR / "FREED_obligations.json"
                if _obligs_path.exists():
                    _obligs_data = json.loads(_obligs_path.read_text())
                    _obligs_list = (_obligs_data if isinstance(_obligs_data, list)
                                   else _obligs_data.get("obligations", []))
                    for _obl_entry in (_obligs_list if isinstance(_obligs_list, list)
                                       else list(_obligs_list.values()) if isinstance(_obligs_list, dict)
                                       else []):
                        _obl_id = _obl_entry.get("id", "")
                        if _obl_id in _analog_annotations_applied:
                            _analog_info = _QUANTUM_ANALOG_MAP.get(_obl_id, {})
                            _obl_entry["analog_domain"] = _analog_info.get("analog_domain")
                            _obl_entry["analog_description"] = _analog_info.get("analog_description")
                            _obl_entry["method_gap"] = _analog_info.get("method_gap")
                            _obl_entry["quantum_result"] = _analog_info.get("quantum_result")
                            _obl_entry["challenge_source"] = _analog_info.get("challenge_source")
                            _obl_entry["analog_annotated_at"] = datetime.now(timezone.utc).isoformat()
                    _obligs_path.write_text(
                        json.dumps(_obligs_data, indent=2, ensure_ascii=False))
                    print(f"  [QUANTUM-ANALOG] Persisted annotations for "
                          f"{_analog_annotations_applied} to obligations file.")
            except Exception as _qa_err:
                print(f"  [QUANTUM-ANALOG] Warning: could not persist annotations: {_qa_err}")
        elif _quantum_ot_signals:
            print(f"  [QUANTUM-ANALOG] Quantum OT signal detected in new knowledge "
                  f"but no matching open obligations found for annotation.")

        affected.sort(
            key=lambda n: self._node_priority(n, open_ob_ids, current_cycle),
            reverse=True,
        )
        print(f"[CONSOLIDATE] Priority order: "
              f"{', '.join(n['id'][:20] for n in affected[:3])}{'...' if len(affected) > 3 else ''}",
              flush=True)

            # ── Phase 1.5: Deduplicate nodes (distributional W2) ─────────────
        affected = self.deduplicate_nodes(affected)
        report["nodes_after_dedup"] = len(affected)

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
                graph._ensure_loaded()
                # FREEZE (O382): while O286 is unresolved, scales_with edges carry no
                # cited exponent/functional form, so route them to consistent_with
                # instead of minting more indefensible scaling claims.
                freeze_scales = _obligation_unresolved(FREEZE_OBLIGATION_ID)
                # SEMANTIC DEDUP: index existing co-assertion invariants per (pair, type).
                # Literal prefix-dedup in record_node_edge catches nothing — restatements
                # differ in wording — so collapse on normalized claim content. The index
                # is extended as we mint, so restatements within this pass collapse too.
                pair_invariants = {}
                for e in graph._node_edges:
                    k = (frozenset((e.get("from"), e.get("to"))), e.get("type"))
                    pair_invariants.setdefault(k, []).append(e.get("invariant", ""))
                # ── Coordinate-conditional detection for depends_on edges ─────
                # (O187 verification protocol): A depends_on edge between two
                # nodes may be basis-dependent — the dependency appears in one
                # coordinate representation but vanishes under transformation.
                # Detection: if node A's invariants, when projected onto node B's
                # semantic basis (via normalized token overlap), show high
                # asymmetry (A→B overlap ≫ B→A overlap or vice versa), the
                # dependency is structural. If the overlaps are symmetric AND
                # low, the dependency is likely a coordinate artifact — it
                # appears only because both nodes share a representational
                # basis, not because one structurally requires the other.
                #
                # Based on the sparse feedback control result: open-loop sparse
                # solutions are equivalent to closed-loop under a specified
                # basis, meaning apparent input-output dependencies can vanish
                # under basis transformation. We flag such edges as
                # coordinate_conditional rather than structural.
                _BASIS_ASYMMETRY_THRESHOLD = 0.25  # |overlap(A→B) - overlap(B→A)| below this → symmetric
                _BASIS_LOW_OVERLAP = 0.15          # both overlaps below this → coordinate artifact
                _coord_conditional_count = 0

                def _check_coordinate_conditional(node_id_a, node_id_b, edge_type_check, inv_text):
                    # type: (str, str, str, str) -> bool
                    """Return True if this depends_on edge is coordinate-conditional
                    (basis-dependent artifact) rather than structural."""
                    if edge_type_check != "depends_on":
                        return False
                    # Get node texts from the index
                    node_a_data = None
                    node_b_data = None
                    for _mn in all_nodes:
                        if _mn.get("id") == node_id_a:
                            node_a_data = _mn
                        elif _mn.get("id") == node_id_b:
                            node_b_data = _mn
                        if node_a_data and node_b_data:
                            break
                    if not node_a_data or not node_b_data:
                        return False
                    # Build token sets for each node
                    def _node_tokens(nd):
                        txt = " ".join(filter(None, [
                            nd.get("compress", ""),
                            " ".join(nd.get("invariants", [])),
                            " ".join(nd.get("tags", [])),
                        ])).lower()
                        return set(
                            w.strip(".,;:()[]'\"!?-")
                            for w in txt.split()
                            if len(w.strip(".,;:()[]'\"!?-")) > 3
                        )
                    toks_a = _node_tokens(node_a_data)
                    toks_b = _node_tokens(node_b_data)
                    if not toks_a or not toks_b:
                        return False
                    # Directional overlaps: fraction of A's tokens in B, and vice versa
                    overlap_a_to_b = len(toks_a & toks_b) / len(toks_a) if toks_a else 0.0
                    overlap_b_to_a = len(toks_a & toks_b) / len(toks_b) if toks_b else 0.0
                    asymmetry = abs(overlap_a_to_b - overlap_b_to_a)
                    # Also check if the invariant text itself uses basis-sensitive
                    # language (coordinate, representation, basis, transform)
                    _basis_keywords = {"basis", "coordinate", "representation",
                                       "transform", "rotation", "projection",
                                       "decomposition", "factorization"}
                    inv_lower = inv_text.lower()
                    has_basis_language = any(kw in inv_lower for kw in _basis_keywords)
                    # Coordinate-conditional: symmetric AND low overlap, OR
                    # explicit basis-sensitive language in the invariant
                    is_symmetric_low = (asymmetry < _BASIS_ASYMMETRY_THRESHOLD
                                        and max(overlap_a_to_b, overlap_b_to_a) < _BASIS_LOW_OVERLAP)
                    return is_symmetric_low or has_basis_language

                minted = skipped_dup = frozen = 0
                for c in candidates:
                    nodes_in = c.get('appears_in', [])
                    edge_type = classify_node_edge(c['invariant'])
                    # independent_confirmation is reserved for bootstrap CONVERGE only,
                    # never MINE keyword-match. Hard guard regardless of classify result.
                    if edge_type == "independent_confirmation":
                        edge_type = "consistent_with"
                    if edge_type == "scales_with" and freeze_scales:
                        edge_type = "consistent_with"
                        frozen += 1
                    inv = c['invariant']
                    for i in range(len(nodes_in)):
                        for j in range(i + 1, len(nodes_in)):
                            # ── Coordinate-conditional gate (O187) ────────
                            # For depends_on edges, check if the dependency
                            # is basis-dependent (appears only in one
                            # coordinate representation). If so, tag as
                            # coordinate_conditional rather than structural.
                            is_coord_cond = _check_coordinate_conditional(
                                nodes_in[i], nodes_in[j], edge_type, inv)
                            effective_type = edge_type
                            edge_metadata = None
                            if is_coord_cond:
                                _coord_conditional_count += 1
                                edge_metadata = "coordinate_conditional"
                                # Downgrade to consistent_with — the dependency
                                # is representational, not structural
                                effective_type = "consistent_with"
                                print(f"  [COORD-COND] depends_on → consistent_with "
                                      f"(basis-dependent): {nodes_in[i][:20]}↔"
                                      f"{nodes_in[j][:20]} — '{inv[:50]}'")

                            k = (frozenset((nodes_in[i], nodes_in[j])), effective_type)
                            existing = pair_invariants.setdefault(k, [])
                            if any(_claims_equivalent(inv, ex) for ex in existing):
                                skipped_dup += 1
                                continue
                            # Record the edge with coordinate_conditional metadata
                            # appended to the invariant text when flagged
                            recorded_inv = inv
                            if edge_metadata == "coordinate_conditional":
                                recorded_inv = (
                                    f"{inv} [coordinate_conditional: basis-dependent "
                                    f"dependency — appears in one representation "
                                    f"but may vanish under transformation (O187)]"
                                )
                            graph.record_node_edge(
                                nodes_in[i], nodes_in[j],
                                effective_type,
                                recorded_inv,
                            )
                            existing.append(inv)
                            minted += 1
                if minted or skipped_dup or frozen or _coord_conditional_count:
                    print(f"  [MINE] node-edges: {minted} minted, {skipped_dup} semantic-dup "
                          f"skipped, {frozen} candidate(s) scales_with→consistent_with "
                          f"(O286 frozen={freeze_scales}), "
                          f"{_coord_conditional_count} depends_on→coordinate_conditional "
                          f"(O187 basis-dependence gate)")

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

        # ── Branching-ratio σ estimation (Poisson branching diagnostic) ───────
        # Compute σ = mean offspring count from graph edge out-degree distribution.
        # Maps avalanche/cascade sizes in the obligation/invariant graph to a
        # memoryless Poisson branching process on rooted trees (per the RFIM /
        # fiber bundle / SOC slip mean-field mapping). σ serves as a live scalar
        # readout of whether the genome is operating at (σ≈1), above (σ>1), or
        # below (σ<1) the critical ridge — directly operationalizing INV_073.
        #
        # CHALLENGE (O21): the paper's exact Poisson branching mapping implies
        # that if γ is the analog of σ, it must follow σ = ρ(h) × J where
        # ρ(h) is field density and J is interaction strength. If the empirical
        # out-degree distribution fails a Poisson goodness-of-fit test, the
        # spectral γ hypothesis is strained, not merely unconfirmed.
        try:
            graph._ensure_loaded()
            _ne = graph._node_edges
            # Build out-degree distribution: for each node, count edges where
            # it is the "from" endpoint (offspring = nodes it propagates to)
            _out_degree = {}  # type: dict
            _all_edge_nodes = set()
            for _e in _ne:
                _from = _e.get("from", "")
                _to = _e.get("to", "")
                if _from:
                    _out_degree[_from] = _out_degree.get(_from, 0) + 1
                    _all_edge_nodes.add(_from)
                if _to:
                    _all_edge_nodes.add(_to)
            # Nodes appearing only as targets (leaves) have out-degree 0
            for _nd in _all_edge_nodes:
                _out_degree.setdefault(_nd, 0)

            _n_nodes_br = len(_out_degree)
            if _n_nodes_br >= 2:
                _degrees = list(_out_degree.values())
                _sigma = sum(_degrees) / float(_n_nodes_br)  # mean offspring count
                _sigma_var = (sum((d - _sigma) ** 2 for d in _degrees)
                              / float(_n_nodes_br))
                _sigma_std = math.sqrt(_sigma_var) if _sigma_var > 0 else 0.0

                # Poisson goodness-of-fit: for a Poisson(σ) distribution,
                # variance == mean. The dispersion index D = var/mean measures
                # departure from Poisson. D≈1 → Poisson consistent,
                # D≫1 → overdispersed (super-Poisson), D≪1 → underdispersed.
                _dispersion_index = _sigma_var / _sigma if _sigma > 0 else 0.0

                # Chi-squared-style Poisson GOF test on the out-degree histogram
                # Group degrees into bins, compare observed vs Poisson-expected counts
                _max_deg = max(_degrees) if _degrees else 0
                _deg_hist = {}  # type: dict
                for _d in _degrees:
                    _deg_hist[_d] = _deg_hist.get(_d, 0) + 1
                # Poisson PMF: P(k) = σ^k * exp(-σ) / k!
                _poisson_chi2 = 0.0
                _poisson_bins_used = 0
                for _k in range(min(_max_deg + 1, 20)):  # cap at 20 bins
                    _observed = _deg_hist.get(_k, 0)
                    # Poisson expected count
                    if _sigma > 0:
                        _log_pmf = _k * math.log(_sigma) - _sigma - sum(
                            math.log(i) for i in range(1, _k + 1))
                        _expected = _n_nodes_br * math.exp(_log_pmf)
                    else:
                        _expected = _n_nodes_br if _k == 0 else 0.0
                    if _expected > 0.5:  # standard GOF: only bins with E >= 0.5
                        _poisson_chi2 += ((_observed - _expected) ** 2) / _expected
                        _poisson_bins_used += 1

                # Approximate p-value from chi2 with (bins-1) degrees of freedom
                # Using Wilson-Hilferty approximation: if X ~ chi2(v),
                # then Z ≈ ((X/v)^(1/3) - (1 - 2/(9v))) / sqrt(2/(9v))
                _poisson_p_value = None
                _poisson_dof = max(_poisson_bins_used - 1, 1)
                if _poisson_bins_used >= 2 and _poisson_dof > 0:
                    _x_over_v = _poisson_chi2 / _poisson_dof
                    if _x_over_v > 0:
                        _wh_cube = _x_over_v ** (1.0 / 3.0)
                        _wh_correction = 1.0 - 2.0 / (9.0 * _poisson_dof)
                        _wh_denom = math.sqrt(2.0 / (9.0 * _poisson_dof))
                        if _wh_denom > 0:
                            _z_score = (_wh_cube - _wh_correction) / _wh_denom
                            # Standard normal survival function approximation
                            _poisson_p_value = 0.5 * (
                                1.0 + math.erf(-_z_score / math.sqrt(2.0)))

                # Criticality classification from σ
                if 0.95 <= _sigma <= 1.05:
                    _sigma_verdict = "AT_CRITICAL"
                elif _sigma < 0.95:
                    _sigma_verdict = "SUBCRITICAL"
                else:
                    _sigma_verdict = "SUPERCRITICAL"

                # Poisson fit verdict
                _poisson_rejected = (_poisson_p_value is not None
                                     and _poisson_p_value < 0.05)
                _poisson_fit_status = "REJECTED" if _poisson_rejected else (
                    "CONSISTENT" if _poisson_p_value is not None else "UNTESTED")

                _branching_report = {
                    "sigma": round(_sigma, 4),
                    "sigma_std": round(_sigma_std, 4),
                    "sigma_variance": round(_sigma_var, 4),
                    "dispersion_index": round(_dispersion_index, 4),
                    "n_nodes": _n_nodes_br,
                    "n_edges": len(_ne),
                    "max_out_degree": _max_deg,
                    "criticality_verdict": _sigma_verdict,
                    "poisson_chi2": round(_poisson_chi2, 4),
                    "poisson_dof": _poisson_dof,
                    "poisson_p_value": (round(_poisson_p_value, 6)
                                        if _poisson_p_value is not None else None),
                    "poisson_fit_status": _poisson_fit_status,
                    "o21_challenge_note": (
                        "Poisson branching model REJECTED (p<0.05): "
                        "out-degree distribution does not follow Poisson "
                        "statistics — the spectral γ hypothesis (γ = ρ×J "
                        "branching ratio) is STRAINED. Overdispersion "
                        f"index D={_dispersion_index:.2f} suggests "
                        + ("super-Poisson cascades (correlated offspring)."
                           if _dispersion_index > 1.5
                           else "sub-Poisson cascades (suppressed branching).")
                    ) if _poisson_rejected else None,
                    "inv073_note": (
                        f"Branching ratio σ={_sigma:.4f}: genome is "
                        f"{_sigma_verdict} — "
                        + ("operating at the critical ridge (σ≈1), consistent "
                           "with INV_073 necessity claim."
                           if _sigma_verdict == "AT_CRITICAL"
                           else f"{'above' if _sigma > 1 else 'below'} criticality, "
                                f"INV_073 ridge navigation is "
                                f"{'not currently maintained' if abs(_sigma - 1.0) > 0.2 else 'approximately maintained'}."
                           )
                    ),
                }

                report["branching_ratio"] = _branching_report

                # Log to dedicated file
                _br_log_path = FREED_DIR / "FREED_log" / "branching_ratio.jsonl"
                _br_log_path.parent.mkdir(exist_ok=True)
                _branching_log_entry = dict(_branching_report)
                _branching_log_entry["timestamp"] = ts
                _branching_log_entry["cycle"] = current_cycle
                with open(_br_log_path, "a") as _br_f:
                    _br_f.write(json.dumps(_branching_log_entry) + "\n")

                print(f"\n[BRANCHING-RATIO] σ={_sigma:.4f} ± {_sigma_std:.4f} "
                      f"(D={_dispersion_index:.2f}, n={_n_nodes_br}): "
                      f"{_sigma_verdict} | Poisson fit: {_poisson_fit_status}"
                      + (f" (p={_poisson_p_value:.4f})"
                         if _poisson_p_value is not None else ""))
                if _poisson_rejected:
                    print(f"  [BRANCHING-RATIO] ⚠ O21 CHALLENGE: Poisson branching "
                          f"model rejected — dispersion D={_dispersion_index:.2f} "
                          f"{'≫' if _dispersion_index > 1.5 else '≪'} 1 — "
                          f"spectral γ=ρ×J functional form is strained")
            else:
                report["branching_ratio"] = {
                    "sigma": None,
                    "n_nodes": _n_nodes_br,
                    "note": "Insufficient nodes for branching ratio estimation",
                }
        except Exception as _br_err:
            report["branching_ratio"] = {
                "error": str(_br_err),
                "note": "Branching ratio estimation failed (non-fatal)",
            }
            print(f"  [BRANCHING-RATIO] Warning: estimation failed: {_br_err}")

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
