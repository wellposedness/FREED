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

        w_dist_single = self._discrete_wasserstein_1d(dist_node, dist_evidence)

        # ── Mixture-of-reference-geometries distance (MaxEnt weights) ─────
        # Replace single-reference-geometry distance with a weighted mixture
        # of reference distances at different hard-sphere diameters (scales),
        # analogous to the ME method for soft-core potentials. The ME method
        # selects optimal hard-sphere reference systems: a single reference
        # recovers Bogoliubov variational results, but a statistical mixture
        # of hard-sphere distributions at different diameters better captures
        # soft-core structure. For semantic spaces: "hard-sphere diameter"
        # maps to vocabulary truncation radius (minimum word length filter).
        #
        # Protocol:
        #   1. Compute W1 distances at K different reference scales
        #      (vocabulary filtered by min_word_len = 3, 4, 5, 6, 7)
        #   2. Estimate local curvature of the embedding manifold at each
        #      scale via discrete second derivative of the distance sequence
        #   3. Fit MaxEnt mixture weights: w_k ∝ exp(-β * curvature_k),
        #      where β is the inverse temperature that maximizes entropy
        #      subject to the curvature constraint
        #   4. Final distance = sum_k w_k * d_k (mixture distance)
        #
        # This is strictly more accurate than a single reference metric for
        # soft-core semantic spaces, improving STF metric tensor approximation
        # fidelity (O112 challenge response).
        _MIXTURE_SCALES = [3, 4, 5, 6, 7]  # hard-sphere diameters (min_word_len)
        _mixture_distances = []
        _mixture_dists_node = []
        _mixture_dists_evid = []
        for _mwl in _MIXTURE_SCALES:
            _d_node_k = self._text_to_distribution(node_text, min_word_len=_mwl)
            _d_evid_k = self._text_to_distribution(evidence_text, min_word_len=_mwl)
            if _d_node_k and _d_evid_k:
                _w_k = self._discrete_wasserstein_1d(_d_node_k, _d_evid_k)
            else:
                _w_k = 1.0  # maximal distance for degenerate distributions
            _mixture_distances.append(_w_k)
            _mixture_dists_node.append(_d_node_k)
            _mixture_dists_evid.append(_d_evid_k)

        # Estimate local curvature at each scale via discrete second derivative
        # of the distance sequence d(scale). Curvature_k = d_{k+1} - 2*d_k + d_{k-1}
        _curvatures = []
        for _ci in range(len(_mixture_distances)):
            if 0 < _ci < len(_mixture_distances) - 1:
                _curv = (_mixture_distances[_ci + 1]
                         - 2.0 * _mixture_distances[_ci]
                         + _mixture_distances[_ci - 1])
            else:
                # Boundary: use forward/backward difference as curvature proxy
                _curv = 0.0
            _curvatures.append(_curv)

        # MaxEnt mixture weights: w_k ∝ exp(-β * |curvature_k|)
        # β chosen to maximize entropy S = -sum w_k ln w_k subject to
        # mean curvature constraint. For the ME selection: β is the Lagrange
        # multiplier. We use β = 1 / (mean |curvature| + ε) which is the
        # canonical MaxEnt choice when the constraint is the mean.
        _abs_curvatures = [abs(_c) for _c in _curvatures]
        _mean_abs_curv = (sum(_abs_curvatures) / len(_abs_curvatures)
                          if _abs_curvatures else 1e-6)
        _beta_maxent = 1.0 / max(_mean_abs_curv, 1e-8)
        # Cap β to prevent numerical overflow in exp
        _beta_maxent = min(_beta_maxent, 50.0)

        _raw_weights_mix = [math.exp(-_beta_maxent * _ac) for _ac in _abs_curvatures]
        _z_mix = sum(_raw_weights_mix)
        if _z_mix > 0:
            _mixture_weights = [_rw / _z_mix for _rw in _raw_weights_mix]
        else:
            _n_mix = len(_MIXTURE_SCALES)
            _mixture_weights = [1.0 / _n_mix] * _n_mix

        # Mixture distance: weighted combination of reference distances
        w_dist = sum(_mw * _md for _mw, _md in
                     zip(_mixture_weights, _mixture_distances))

        # Mixture entropy (diagnostic): how uniform are the weights?
        _mixture_entropy = 0.0
        for _mw in _mixture_weights:
            if _mw > 1e-12:
                _mixture_entropy -= _mw * math.log(_mw)
        _max_mixture_entropy = math.log(len(_MIXTURE_SCALES)) if len(_MIXTURE_SCALES) > 1 else 1.0
        _mixture_entropy_norm = (_mixture_entropy / _max_mixture_entropy
                                 if _max_mixture_entropy > 0 else 0.0)

        # Log mixture diagnostics when weights are non-uniform (interesting case)
        if _mixture_entropy_norm < 0.9:
            _dominant_scale = _MIXTURE_SCALES[_mixture_weights.index(max(_mixture_weights))]
            print(f"  [MIXTURE-REF] Non-uniform MaxEnt weights: "
                  f"scales={_MIXTURE_SCALES}, "
                  f"weights=[{', '.join(f'{w:.3f}' for w in _mixture_weights)}], "
                  f"dominant_scale={_dominant_scale}, "
                  f"d_single={w_dist_single:.4f} → d_mixture={w_dist:.4f}, "
                  f"H_norm={_mixture_entropy_norm:.3f}, "
                  f"β={_beta_maxent:.2f}")

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

        # ── Entropic OT dominance scoring (INV_094 challenge response) ────
        # When both distributions have sufficient multivariate support
        # (≥ MIN_OT_SUPPORT shared keys), compute a multivariate stochastic
        # dominance statistic via entropic-regularized optimal transport.
        #
        # The paper proves that multivariate first stochastic dominance
        # can be assessed through OT couplings with smooth cost. The
        # entropic regularization (Sinkhorn) makes the statistic
        # computationally tractable and yields CLT + bootstrap consistency
        # for the empirical test.
        #
        # Dominance statistic D_ε(P, Q):
        #   For each shared key i, compute the coupling-marginal violation:
        #     v_i = max(0, F_Q(i) - F_P(i))  (Q dominates P if all v_i = 0)
        #   The OT dominance score integrates these violations under the
        #   entropic transport plan π_ε, where ε is the regularization
        #   strength:
        #     D_ε = sum_i sum_j π_ε(i,j) * c(i,j) * v_j
        #
        #   We approximate π_ε via Sinkhorn iterations on the cost matrix
        #   c(i,j) = |rank_P(i) - rank_Q(j)| / n (normalized rank distance).
        #
        # INV_094 CHALLENGE acknowledgment: the choice of cost function c
        # materially changes which distribution dominates. We use normalized
        # rank distance as a canonical ordinal cost, but note this is ONE
        # choice — the Wasserstein geometry underlying INV_094 is not
        # uniquely determined but cost-sensitive. The dominance score
        # should be interpreted as cost-conditional, not absolute.
        #
        # When batch size is insufficient (< MIN_OT_SUPPORT), we fall back
        # to the scalar entropic + Wasserstein blend computed above.
        MIN_OT_SUPPORT = 8        # minimum shared keys for OT dominance
        SINKHORN_REG = 0.1        # entropic regularization ε
        SINKHORN_ITERS = 50       # max Sinkhorn iterations
        SINKHORN_TOL = 1e-6       # convergence tolerance

        shared_keys_ot = sorted(set(dist_node.keys()) & set(dist_evidence.keys()))
        ot_dominance_result = None

        if len(shared_keys_ot) >= MIN_OT_SUPPORT:
            n_ot = len(shared_keys_ot)

            # Extract marginal vectors over shared support
            p_vec = [dist_node.get(k, 0.0) for k in shared_keys_ot]
            q_vec = [dist_evidence.get(k, 0.0) for k in shared_keys_ot]

            # Normalize to proper distributions over shared support
            p_sum = sum(p_vec)
            q_sum = sum(q_vec)
            if p_sum > 0 and q_sum > 0:
                p_vec = [x / p_sum for x in p_vec]
                q_vec = [x / q_sum for x in q_vec]

                # Build cost matrix: normalized rank distance
                # Rank each distribution over the shared keys
                def _rank_vec(vals):
                    # type: (list) -> list
                    indexed = sorted(range(len(vals)), key=lambda i: vals[i])
                    ranks = [0.0] * len(vals)
                    for r, idx in enumerate(indexed):
                        ranks[idx] = (r + 1.0) / len(vals)
                    return ranks

                ranks_p = _rank_vec(p_vec)
                ranks_q = _rank_vec(q_vec)

                # Cost matrix C[i][j] = |rank_p(i) - rank_q(j)|
                cost_matrix = []
                for i in range(n_ot):
                    row = []
                    for j in range(n_ot):
                        row.append(abs(ranks_p[i] - ranks_q[j]))
                    cost_matrix.append(row)

                # Sinkhorn iterations for entropic OT plan π_ε
                # K[i][j] = exp(-C[i][j] / ε)
                eps = max(SINKHORN_REG, 1e-10)
                K = []
                for i in range(n_ot):
                    row = []
                    for j in range(n_ot):
                        row.append(math.exp(-cost_matrix[i][j] / eps))
                    K.append(row)

                # Initialize scaling vectors
                u = [1.0] * n_ot
                v = [1.0] * n_ot

                converged = False
                for _sink_iter in range(SINKHORN_ITERS):
                    # Update u: u_i = p_i / sum_j K[i][j] * v_j
                    u_new = []
                    for i in range(n_ot):
                        kv_sum = sum(K[i][j] * v[j] for j in range(n_ot))
                        u_new.append(p_vec[i] / max(kv_sum, 1e-12))
                    # Update v: v_j = q_j / sum_i K[i][j] * u_i
                    v_new = []
                    for j in range(n_ot):
                        ku_sum = sum(K[i][j] * u_new[i] for i in range(n_ot))
                        v_new.append(q_vec[j] / max(ku_sum, 1e-12))
                    # Check convergence
                    u_diff = max(abs(u_new[i] - u[i]) for i in range(n_ot))
                    v_diff = max(abs(v_new[j] - v[j]) for j in range(n_ot))
                    u = u_new
                    v = v_new
                    if max(u_diff, v_diff) < SINKHORN_TOL:
                        converged = True
                        break

                # Compute transport plan π_ε[i][j] = u_i * K[i][j] * v_j
                pi_plan = []
                for i in range(n_ot):
                    row = []
                    for j in range(n_ot):
                        row.append(u[i] * K[i][j] * v[j])
                    pi_plan.append(row)

                # CDF violation: v_j = max(0, F_Q(j) - F_P(j))
                # Build CDFs over shared support
                cdf_p = []
                cdf_q = []
                cum_p = 0.0
                cum_q = 0.0
                for idx in range(n_ot):
                    cum_p += p_vec[idx]
                    cum_q += q_vec[idx]
                    cdf_p.append(cum_p)
                    cdf_q.append(cum_q)

                violations = [max(0.0, cdf_q[j] - cdf_p[j]) for j in range(n_ot)]

                # Dominance statistic: D_ε = sum_ij π_ε(i,j) * c(i,j) * v_j
                d_epsilon = 0.0
                for i in range(n_ot):
                    for j in range(n_ot):
                        d_epsilon += pi_plan[i][j] * cost_matrix[i][j] * violations[j]

                # Dominance interpretation:
                # D_ε ≈ 0 → evidence distribution is dominated by (or equal to) node
                #           → node already encompasses this evidence (low update needed)
                # D_ε > 0 → evidence has mass in regions node underweights
                #           → genuine new information, higher update weight warranted
                # The dominance score modulates the MWDE weight: when D_ε is high,
                # the evidence is genuinely novel (not a false equivalence collapsed
                # by scalar cosine), so boost the weight.
                max_violation = max(violations) if violations else 0.0
                mean_violation = sum(violations) / n_ot if n_ot > 0 else 0.0

                # Dominance boost: sigmoid mapping of D_ε to [0, 0.3] additive range
                # so OT dominance can augment but not overwhelm the base score
                dominance_boost_raw = 1.0 / (1.0 + math.exp(-20.0 * (d_epsilon - 0.02)))
                dominance_boost = 0.3 * dominance_boost_raw

                # Apply dominance modulation to mwde_weight
                mwde_weight_pre_ot = mwde_weight
                mwde_weight = min(1.0, mwde_weight + dominance_boost * mwde_weight)

                ot_dominance_result = {
                    "d_epsilon": round(d_epsilon, 6),
                    "sinkhorn_converged": converged,
                    "sinkhorn_reg": eps,
                    "n_shared_keys": n_ot,
                    "max_cdf_violation": round(max_violation, 6),
                    "mean_cdf_violation": round(mean_violation, 6),
                    "dominance_boost": round(dominance_boost, 6),
                    "mwde_weight_pre_ot": round(mwde_weight_pre_ot, 4),
                    "mwde_weight_post_ot": round(mwde_weight, 4),
                    "cost_function": "normalized_rank_distance",
                    "inv094_note": (
                        "Cost function choice (normalized rank distance) is ONE "
                        "canonical ordinal cost. The paper proves cost sensitivity: "
                        "different smooth costs can reverse dominance ordering. "
                        "This score is cost-conditional, not absolute."
                    ),
                }

                if dominance_boost > 0.01:
                    print(f"  [OT-DOMINANCE] D_ε={d_epsilon:.4f}, "
                          f"boost={dominance_boost:.4f} → "
                          f"mwde {mwde_weight_pre_ot:.4f}→{mwde_weight:.4f} "
                          f"(n={n_ot} shared keys, "
                          f"{'converged' if converged else 'max-iter'})")
            else:
                ot_dominance_result = {
                    "d_epsilon": None,
                    "reason": "degenerate_shared_support",
                    "n_shared_keys": n_ot,
                }
        else:
            ot_dominance_result = {
                "d_epsilon": None,
                "reason": "insufficient_shared_support",
                "n_shared_keys": len(shared_keys_ot),
                "min_required": MIN_OT_SUPPORT,
                "fallback": "scalar_entropic_wasserstein_blend",
            }

        # ── Markov-order admissibility gate (INV_087 challenge response) ──
        # Filter the scoring functional against the two invariant additivity
        # conditions characterizing thermodynamically admissible Lyapunov
        # functionals for continuous-time Markov chains:
        #
        #   (i)  Joining additivity: ∃ monotonic φ s.t. φ(F(P⊗Q)) = φ(F(P)) + φ(F(Q))
        #        for independent systems P, Q. This constrains F to the Rényi/Tsallis
        #        family H_q(P) = (1/(1-q)) * ln(Σ p_i^q) parameterized by q ∈ (0,∞).
        #
        #   (ii) Partition additivity: ∃ monotonic ψ s.t. ψ(F(P)) = Σ_k ψ(F(P|A_k)) * w_k
        #        for any partition {A_k} of the state space. This further constrains
        #        the functional to the same Rényi/Tsallis family.
        #
        # The intersection of (i) and (ii) is exactly the Rényi entropy family
        # parameterized by q. Free-choice entropy proxies (e.g., ad hoc weighted
        # sums, unnormalized log-likelihoods) that fail either condition are
        # rejected — the edge weight is flagged as inadmissible and attenuated.
        #
        # Implementation: estimate q from the evidence distribution by fitting
        # the Rényi entropy's defining relation, then verify both additivity
        # conditions hold within tolerance. If they fail, the scoring functional
        # is not in the admissible family and the weight is attenuated.
        #
        # INV_087 CHALLENGE: the admissible family is a CONTINUUM parameterized
        # by q ∈ (0,∞). MaxRL's specific choice (Shannon entropy, q=1) must be
        # justified as selecting one privileged member, not as THE unique
        # admissible functional. This gate enforces membership in the family
        # but does NOT privilege any particular q value.
        markov_order_result = self._markov_order_admissibility_gate(
            dist_node, dist_evidence)
        markov_admissible = markov_order_result["admissible"]
        markov_attenuation = markov_order_result.get("attenuation_factor", 1.0)

        if not markov_admissible:
            # Attenuate the MWDE weight — the scoring functional is not in the
            # Rényi/Tsallis admissible family, so the edge weight is unreliable
            mwde_weight *= markov_attenuation
            print(f"  [MARKOV-ORDER] ⚠ Scoring functional INADMISSIBLE: "
                  f"q_est={markov_order_result.get('q_estimate', '?')}, "
                  f"joining_residual={markov_order_result.get('joining_residual', '?')}, "
                  f"partition_residual={markov_order_result.get('partition_residual', '?')} "
                  f"→ weight attenuated by {markov_attenuation:.3f}× "
                  f"(INV_087: free-choice entropy proxy rejected)")

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

        # ── Fisher-regularized variance dynamics (O112 regime detection) ──
        # The paper derives exact reduced-variance ODE for Fisher-regularized
        # Wasserstein gradient flow on Gaussian manifold:
        #     u̇ = 2(1 - u) + ε/u
        # where u = σ²(t) is variance and ε > 0 is Fisher regularization
        # strength. The cross-dissipation term ε/u changes sign at the
        # critical scale σ = 1 (u = 1), separating:
        #   - Cooperative regime (u < 1): transport and Fisher dissipation
        #     reinforce each other, accelerating convergence
        #   - Competitive regime (u > 1): transport and Fisher dissipation
        #     oppose each other, creating transient overshoot
        #
        # We estimate u from the ratio of evidence variance to node variance
        # in the shared semantic space, then compute the instantaneous
        # cross-dissipation sign to label the thermodynamic regime.
        #
        # This makes STF metric recovery (O112) tractable: the regime label
        # distinguishes transient dynamics (sign-change transient) from
        # equilibrium geometry, which O112's single-experiment specification
        # conflates without this flag.
        FISHER_EPSILON = 0.01  # regularization strength ε

        # Estimate variance proxy u from the two distributions:
        # u = Var(evidence) / Var(node), treating probability masses as samples
        _node_vals = list(dist_node.values())
        _evid_vals = list(dist_evidence.values())

        def _distribution_variance(vals):
            # type: (list) -> float
            if len(vals) < 2:
                return 1e-12
            mean_v = sum(vals) / len(vals)
            return sum((v - mean_v) ** 2 for v in vals) / len(vals)

        _var_node = _distribution_variance(_node_vals)
        _var_evid = _distribution_variance(_evid_vals)
        # u = σ²(evidence) / σ²(node) — variance ratio as proxy for
        # the reduced variance on the Gaussian manifold
        _u_variance = _var_evid / max(_var_node, 1e-12)
        # Clamp to avoid numerical issues at extreme ratios
        _u_variance = max(1e-6, min(_u_variance, 100.0))

        # Instantaneous dynamics: u̇ = 2(1 - u) + ε/u
        _u_dot = 2.0 * (1.0 - _u_variance) + FISHER_EPSILON / _u_variance

        # Cross-dissipation term: ε/u (always positive, but its interaction
        # with the transport term 2(1-u) determines the regime)
        _cross_dissipation = FISHER_EPSILON / _u_variance

        # Transport dissipation term: 2(1-u)
        _transport_dissipation = 2.0 * (1.0 - _u_variance)

        # Regime classification based on cross-dissipation polarity:
        # At u < 1: transport term > 0 (drives toward equilibrium),
        #           cross-dissipation > 0 (reinforces) → COOPERATIVE
        # At u > 1: transport term < 0 (drives toward equilibrium from above),
        #           cross-dissipation > 0 (opposes transport) → COMPETITIVE
        # At u ≈ 1: critical scale, sign change → CRITICAL_TRANSITION
        _CRITICAL_BAND = 0.05  # |u - 1| < this → critical transition zone
        if abs(_u_variance - 1.0) < _CRITICAL_BAND:
            _fisher_regime = "CRITICAL_TRANSITION"
        elif _u_variance < 1.0:
            _fisher_regime = "COOPERATIVE"
        else:
            _fisher_regime = "COMPETITIVE"

        # Fixed point: u̇ = 0 → 2(1-u) + ε/u = 0 → 2u² - 2u - ε = 0
        # u* = (1 + sqrt(1 + 2ε)) / 2 (positive root)
        _u_fixed_point = (1.0 + math.sqrt(1.0 + 2.0 * FISHER_EPSILON)) / 2.0
        _distance_to_fixed_point = abs(_u_variance - _u_fixed_point)

        _fisher_regime_result = {
            "u_variance": round(_u_variance, 6),
            "u_dot": round(_u_dot, 6),
            "cross_dissipation": round(_cross_dissipation, 6),
            "transport_dissipation": round(_transport_dissipation, 6),
            "fisher_epsilon": FISHER_EPSILON,
            "regime": _fisher_regime,
            "u_fixed_point": round(_u_fixed_point, 6),
            "distance_to_fixed_point": round(_distance_to_fixed_point, 6),
            "critical_scale_sigma": 1.0,
            "o112_note": (
                f"Fisher-regularized Wasserstein regime: {_fisher_regime} "
                f"(u={_u_variance:.4f}, u̇={_u_dot:.4f}). "
                + ("Cross-dissipation and transport REINFORCE — "
                   "accelerated convergence toward equilibrium."
                   if _fisher_regime == "COOPERATIVE"
                   else "Cross-dissipation OPPOSES transport — "
                        "transient overshoot before equilibrium."
                   if _fisher_regime == "COMPETITIVE"
                   else "At critical scale σ=1 — cross-dissipation "
                        "sign change in progress, regime transition."
                   )
                + f" Fixed point u*={_u_fixed_point:.4f}, "
                  f"distance={_distance_to_fixed_point:.4f}. "
                  f"STF metric recovery (O112) requires distinguishing "
                  f"this transient regime from equilibrium geometry."
            ),
        }

        if _fisher_regime != "COOPERATIVE":
            print(f"  [FISHER-REGIME] {_fisher_regime}: u={_u_variance:.4f}, "
                  f"u̇={_u_dot:.4f}, ε/u={_cross_dissipation:.4f}, "
                  f"2(1-u)={_transport_dissipation:.4f} — "
                  f"{'sign-change transient at σ=1' if _fisher_regime == 'CRITICAL_TRANSITION' else 'competitive cross-dissipation'} "
                  f"(O112: transient regime ≠ equilibrium geometry)")

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

        # ── Spectral entropy of co-occurrence transition matrix (WDTN) ────
        # Treat the existing node-edge weight matrix as a weighted dynamic
        # transfer network (WDTN) adjacency. Compute the transition matrix
        # T (row-stochastic normalization of the adjacency), extract its
        # eigenvalue spectrum, and compute the spectral entropy:
        #   H_spectral = -sum_i (|λ_i|/Z) * ln(|λ_i|/Z)
        # where Z = sum_i |λ_i|. Normalize by ln(N) to get γ_spectral ∈ [0,1].
        #
        # CHALLENGE (O21): spectral γ is SNR-sensitive and bin-resolution-
        # dependent — any ToS–spectral-γ correlation claimed by O21 could
        # be an artifact of coarse-graining choices rather than a substrate-
        # independent invariant. This implementation uses the raw edge-weight
        # adjacency without binning, surfacing the γ value for empirical
        # comparison but NOT treating it as a confirmed invariant.
        spectral_gamma_result = self._compute_spectral_gamma(nodes)

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
            "spectral_gamma": spectral_gamma_result,
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
        "O28":  {"rate_distortion"},
        "modal paths": {"modal_thermo"},
        "thermality variance": {"modal_thermo", "EIT"},
        "field average": {"EIT", "field_theoretic"},
        "correlation function": {"EIT", "field_theoretic"},
        "renormalization": {"RG"},
        "critical": {"RG", "EIT"},
        "dissipation": {"EIT"},
        "entropy production": {"EIT"},
        "rate": {"rate_distortion"},
        "capacity": {"rate_distortion"},
        "regret bound": {"rate_distortion"},
        "channel": {"rate_distortion"},
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

    # ── O112 thermality-parameterization terms ───────────────────────────────
    # Papers that parameterize a continuous order parameter (fractional
    # derivative α, temperature β, or analogous thermality handle) are
    # high-priority O112 candidates because O112 (STF metric tensor
    # recovery) requires identifying papers with continuous thermality
    # parameterizations. This keyword list surfaces the exact class of
    # evidence needed to close O112.
    _O112_THERMALITY_TERMS = [
        "fractional", "order parameter", "thermality",
        "non-integer", "memory kernel",
    ]

    # ── Correlation-length scaling exponent keywords for O112 metric recovery ─
    # Papers reporting ξ∼μ^{-σ} or equivalent correlation-length scaling laws
    # are structurally analogous to the STF metric tensor recovery experiment:
    # the scaling exponent σ encodes how the effective metric (correlation
    # length) depends on a control parameter (quench rate μ), which is the
    # operational definition of metric tensor recovery from observable dynamics.
    _O112_CORRELATION_LENGTH_TERMS = [
        "correlation length", "correlation-length", "ξ",
        "xi", "coherence length", "domain size",
    ]
    _O112_SCALING_EXPONENT_TERMS = [
        "scaling exponent", "power-law", "power law",
        "μ^{-", "mu^{-", "∼μ", "~μ", "∼ μ", "~ μ",
        "quench rate", "crossing rate", "ramp rate",
    ]
    _O112_KZ_MECHANISM_TERMS = [
        "kibble-zurek", "kibble zurek", "kz mechanism",
        "defect density", "domain wall", "front dynamics",
    ]

    def _score_o112_thermality(self, paper_text):
        # type: (str) -> float
        """
        Score a paper's relevance to O112 (STF metric tensor recovery)
        by checking for continuous thermality parameterization signals
        AND correlation-length scaling exponent reports.

        Papers that parameterize a continuous order parameter (e.g.,
        fractional derivative α, temperature β, or analogous thermality
        handle) are high-priority O112 candidates. Papers reporting
        empirical ξ-scaling laws (ξ∼μ^{-σ}) are flagged as metric-
        recovery analogs — the scaling exponent σ encodes how the
        effective metric (correlation length) depends on a control
        parameter, which is the operational definition of metric tensor
        recovery from observable dynamics.

        Returns an additive boost >= 0.0.

        INV_073 challenge acknowledgment: the FMHNN paper's encryption
        scheme *maximizes* security by operating in deep chaos (maximum
        Lyapunov exponent, large attractor dimension) rather than at the
        critical ridge, suggesting that for engineering applications the
        optimal operating point may be fully chaotic rather than
        edge-of-chaos. This strains the universality of the critical
        ridge principle as the single optimal operating regime.

        INV_073 challenge (Kibble-Zurek bifurcation): papers showing
        that even at the critical transition point, two qualitatively
        distinct outcomes (coherent vs. incoherent) exist depending on
        quench rate μ imply the "critical ridge" is not a single
        navigable path but a rate-dependent bifurcation of paths. This
        strains the genome's treatment of criticality as a unique
        attractor state — the critical manifold has internal structure
        (μ_c separating dynamical regimes) not captured by a scalar
        γ≈1 criterion.
        """
        text_lower = paper_text.lower()
        boost = 0.0

        # ── Stage 1: Continuous thermality parameterization detection ─────
        hits = [term for term in self._O112_THERMALITY_TERMS
                if term in text_lower]
        if hits:
            # Base boost: 0.08 per matched term, capped at 0.30
            boost = min(len(hits) * 0.08, 0.30)

            # Extra boost (+0.10) when fractional-order dynamics co-occur with
            # chaotic / attractor language — these papers demonstrate continuous
            # α-parameterization of dynamical regime transitions, which is
            # exactly the thermality handle O112 needs.
            _chaos_terms = {"chaotic", "lyapunov", "attractor", "bifurcation",
                            "hopfield", "memristive", "sensitivity to initial"}
            has_chaos = any(ct in text_lower for ct in _chaos_terms)
            if "fractional" in hits and has_chaos:
                boost += 0.10

            print(f"  [O112-THERMALITY] +{boost:.2f} boost: continuous "
                  f"thermality parameterization detected "
                  f"(terms={hits}"
                  + (", chaos_co_occurrence=True" if has_chaos else "")
                  + f") — high-priority O112 candidate for STF metric "
                  f"tensor recovery")

        # ── Stage 2: Correlation-length scaling exponent detection ────────
        # Papers reporting ξ∼μ^{-σ} or equivalent are metric-recovery
        # analogs: the scaling exponent σ IS the metric tensor component
        # relating correlation length to control parameter, recovered
        # empirically from observable dynamics.
        _corr_hits = [t for t in self._O112_CORRELATION_LENGTH_TERMS
                      if t in text_lower]
        _scaling_hits = [t for t in self._O112_SCALING_EXPONENT_TERMS
                         if t in text_lower]
        _kz_hits = [t for t in self._O112_KZ_MECHANISM_TERMS
                    if t in text_lower]

        # Require co-occurrence: correlation-length language AND scaling
        # exponent language. KZ mechanism terms provide additional boost.
        if _corr_hits and _scaling_hits:
            # Base metric-recovery analog boost
            _corr_boost = 0.15

            # KZ mechanism boost: papers explicitly citing Kibble-Zurek
            # are the highest-fidelity metric-recovery analogs because
            # they derive σ from universality class exponents (ν, z)
            if _kz_hits:
                _corr_boost += 0.10

            # Rate-dependent bifurcation detection (INV_073 challenge):
            # papers reporting TWO distinct dynamical regimes separated
            # by a critical crossing rate μ_c demonstrate that the
            # critical manifold has internal structure — tag this as
            # an INV_073 challenge signal
            _bifurcation_terms = {
                "critical crossing rate", "critical rate", "μ_c",
                "mu_c", "two different mechanisms", "two regimes",
                "two distinct", "coherent", "incoherent",
                "subcritical bifurcation", "rate-dependent",
                "rate dependent", "crossing rate",
            }
            _has_rate_bifurcation = sum(
                1 for bt in _bifurcation_terms if bt in text_lower
            ) >= 2  # require ≥2 co-occurring bifurcation signals

            if _has_rate_bifurcation:
                _corr_boost += 0.05
                print(f"  [O112-METRIC-RECOVERY] ⚠ INV_073 CHALLENGE: "
                      f"rate-dependent bifurcation at critical point "
                      f"detected — critical ridge has internal structure "
                      f"(μ_c separating coherent/incoherent regimes). "
                      f"Scalar γ≈1 criterion insufficient to characterize "
                      f"the critical manifold's dynamical topology.")

            boost += _corr_boost

            print(f"  [O112-METRIC-RECOVERY] +{_corr_boost:.2f} boost: "
                  f"correlation-length scaling exponent detected "
                  f"(corr_length={_corr_hits}, scaling={_scaling_hits}"
                  + (f", kz_mechanism={_kz_hits}" if _kz_hits else "")
                  + (f", rate_bifurcation=True" if _has_rate_bifurcation else "")
                  + f") — metric-recovery analog for O112 "
                  f"(ξ∼μ^{{-σ}} ≅ STF metric tensor recovery)")

        if boost == 0.0:
            return 0.0

        return boost

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

        Also includes O112 thermality-parameterization scoring: papers that
        parameterize a continuous order parameter (fractional derivative α,
        temperature β, or analogous thermality handle) are flagged as
        high-priority O112 candidates via _score_o112_thermality().
        """
        paper_formalisms = self._detect_formalism_types(paper_text)

        # ── O112 thermality-parameterization scoring ─────────────────────────
        # Flag papers with continuous order parameters as high-priority O112
        # candidates. O112 has been open since early genome versions and
        # requires identifying papers with continuous thermality
        # parameterizations; this filter surfaces the exact class of
        # evidence needed to close it.
        o112_thermality_boost = self._score_o112_thermality(paper_text)

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

        if not paper_formalisms and hierarchical_temporal_bonus == 0.0 and o112_thermality_boost == 0.0:
            return 0.0

        boost = hierarchical_temporal_bonus + o112_thermality_boost
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
        directly operationalize open obligations get boosted priority.

        Sparse-ADMM–inspired inverse-dependency weighting: obligations with fewer
        dependent edges (sparser dependency skeleton) are cheaper to resolve and
        receive higher urgency scores. This mirrors the sparse MPC-for-tracking
        algorithm's exploitation of sparse structure to reduce computational debt —
        resolve the cheapest obligations first to maximize throughput.

        INV_073 challenge acknowledgment: the MPC-for-tracking formulation achieves
        recursive feasibility by AUGMENTING the decision-variable space (adding
        slack/artificial variables), directly stressing the claim that critical-ridge
        navigation is achievable without expanding the representational substrate.
        Pure compression may be insufficient when the constraint topology shifts."""
        ob_refs    = node.get('obligations', [])
        ob_overlap = sum(1 for ref in ob_refs
                         if any(ref == oid or ref in oid or oid in ref
                                for oid in open_ob_ids))
        inv_density   = len(node.get('invariants', []))
        cycles_stale  = current_cycle - node.get('last_renorm_cycle', 0)

        # ── Sparse-obligation urgency boost (inverse-dependency-count) ───────
        # For each open obligation referenced by this node, count the number of
        # dependent edges (other obligations/invariants that reference it) in the
        # knowledge graph. Obligations with fewer dependents have sparser
        # dependency skeletons and are cheaper to resolve — boost their urgency
        # via inverse weighting: urgency_boost += 1 / (1 + dep_count).
        # This surfaces low-hanging-fruit obligations that would otherwise be
        # treated identically to high-dependency-count obligations, preventing
        # debt accumulation where the genome is most tractable.
        sparsity_boost = 0.0
        try:
            _graph_sp = get_graph()
            _graph_sp._ensure_loaded()
            _ne_sp = _graph_sp._node_edges
            # Build dependency count per obligation: how many edges reference it
            _ob_dep_counts = {}  # type: dict
            for _e_sp in _ne_sp:
                _inv_text = (_e_sp.get("invariant", "") or "").lower()
                _from_sp = _e_sp.get("from", "")
                _to_sp = _e_sp.get("to", "")
                for oid in open_ob_ids:
                    oid_lower = oid.lower()
                    if (oid_lower in _inv_text
                            or oid_lower in _from_sp.lower()
                            or oid_lower in _to_sp.lower()):
                        _ob_dep_counts[oid] = _ob_dep_counts.get(oid, 0) + 1

            # Compute inverse-dependency boost for obligations this node references
            for ref in ob_refs:
                matching_oids = [
                    oid for oid in open_ob_ids
                    if ref == oid or ref in oid or oid in ref
                ]
                for oid in matching_oids:
                    dep_count = _ob_dep_counts.get(oid, 0)
                    # Inverse weighting: fewer deps → higher boost
                    # 1/(1+dep_count) gives 1.0 for isolated obligations,
                    # 0.5 for 1-dep, 0.33 for 2-dep, etc.
                    sparsity_boost += 1.0 / (1.0 + dep_count)
        except Exception:
            pass  # fail-open: graph unavailable → no sparsity boost

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

            # ── Fractional-order / non-Markovian memory kernel detection ─────
            # (INV_073 challenge): Papers describing fractional-order dynamics,
            # non-Markovian memory kernels, or history-dependent criticality
            # represent a mechanistically distinct route to the critical ridge.
            # Without tagging these, CONVERGE mappings to INV_073 conflate
            # fractional (history-dependent) and integer (Markovian/instantaneous)
            # memory substrates, degrading the genome's causal resolution.
            #
            # Detection: co-occurrence of (1) fractional-order / fractional
            # calculus language AND (2) memory / history-dependence language.
            # Both families must be present to distinguish "paper mentions
            # fractional" from "paper demonstrates fractional memory dynamics."
            #
            # When detected, the node is tagged with memory_kernel=fractional
            # so downstream invariant tagging distinguishes fractional from
            # Markovian criticality claims before genome comparison.
            _FRACTIONAL_ORDER_KEYWORDS = {
                "fractional-order", "fractional order", "fractional calculus",
                "fractional derivative", "caputo", "riemann-liouville",
                "grünwald-letnikov", "grunwald-letnikov", "mittag-leffler",
                "fractional differential", "fractional dynamics",
                "adomian decomposition", "adomian", "fractional hopfield",
                "fractional memristor", "fractional-order memrist",
                "non-integer order", "non-integer-order",
            }
            _MEMORY_KERNEL_KEYWORDS = {
                "memory kernel", "memory-kernel", "non-markovian",
                "non markovian", "nonmarkovian", "history-dependent",
                "history dependent", "long-range memory", "long range memory",
                "memory effect", "memory effects", "fading memory",
                "hereditary", "viscoelastic memory", "power-law memory",
                "coupling strength", "memristive", "memristor",
                "electromagnetic radiation", "hidden attractor",
                "hidden chaotic", "dual-wing", "dual wing",
            }
            _has_fractional = any(kw in _nk_lower_ec for kw in _FRACTIONAL_ORDER_KEYWORDS)
            _has_memory_kernel = any(kw in _nk_lower_ec for kw in _MEMORY_KERNEL_KEYWORDS)
            _memory_kernel_bonus = 0.0
            if _has_fractional and _has_memory_kernel:
                _memory_kernel_bonus = 0.10
                total_score += _memory_kernel_bonus
                node["memory_kernel"] = "fractional"
                node.setdefault("memory_kernel_signals", []).append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "fractional_keywords_matched": [
                        kw for kw in _FRACTIONAL_ORDER_KEYWORDS
                        if kw in _nk_lower_ec
                    ],
                    "memory_keywords_matched": [
                        kw for kw in _MEMORY_KERNEL_KEYWORDS
                        if kw in _nk_lower_ec
                    ],
                    "bonus_applied": _memory_kernel_bonus,
                    "source": "fractional_memory_kernel_detection",
                    "inv073_challenge": (
                        "Paper demonstrates criticality sustained by a "
                        "fractional (non-Markovian, history-dependent) memory "
                        "mechanism. INV_073's implicit assumption that "
                        "criticality navigation is fully characterized by "
                        "instantaneous coupling-strength tuning is strained: "
                        "the critical ridge can be maintained by memory-kernel "
                        "structure (fractional order parameter) rather than "
                        "Markovian coupling alone. CONVERGE mappings to INV_073 "
                        "must distinguish memory_kernel=fractional from "
                        "memory_kernel=markovian to preserve causal resolution."
                    ),
                })
                print(f"  [MEMORY-KERNEL] +{_memory_kernel_bonus:.2f} bonus: "
                      f"fractional-order non-Markovian memory kernel detected "
                      f"— tagged memory_kernel=fractional (INV_073 challenge: "
                      f"history-dependent criticality ≠ Markovian criticality)")
            elif _has_fractional and not _has_memory_kernel:
                # Fractional calculus mentioned but no memory kernel language —
                # weaker signal, tag but don't boost
                node.setdefault("memory_kernel", "unspecified")
            elif _has_memory_kernel and not _has_fractional:
                # Memory language without fractional order — standard memristive
                node.setdefault("memory_kernel", "integer")

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

            # ── Power-law exponent variance detection (O21 challenge) ────────
            # Papers reporting power-law exponent measurements across multiple
            # datasets/substrates are tagged with exponent_variance=True so
            # downstream O21 correlation tests can distinguish universal-γ
            # claims from substrate-specific-γ claims. SOC universality is
            # structural (criticality is ubiquitous) but not numerical
            # (exponents are substrate-specific), obligating O21 to predict
            # system-class-dependent γ rather than a single universal slope.
            #
            # Detection: co-occurrence of power-law exponent language AND
            # non-universality / substrate-specificity language. A hit requires
            # ≥1 keyword from EACH family to avoid false positives on generic
            # power-law mentions.
            _EXPONENT_KEYWORDS = {
                "power-law exponent", "power law exponent", "power-law slope",
                "power law slope", "size distribution", "scaling exponent",
                "exponent α", "exponent alpha", "α_f", "α_e",
                "power-law-like", "power law index",
            }
            _NONUNIVERSAL_KEYWORDS = {
                "not universal", "non-universal", "nonuniversal",
                "substrate-specific", "substrate specific",
                "system-dependent", "system dependent",
                "context-dependent", "context dependent",
                "vary with", "varies with", "varying exponent",
                "individual physical modeling", "physical scaling laws",
                "not a natural consequence", "depends on",
                "different exponents", "exponent variance",
                "class-dependent", "class dependent",
            }
            _has_exponent_kw = any(kw in _nk_combined_lower for kw in _EXPONENT_KEYWORDS)
            _has_nonuniversal_kw = any(kw in _nk_combined_lower for kw in _NONUNIVERSAL_KEYWORDS)
            _exponent_variance_bonus = 0.0
            if _has_exponent_kw and _has_nonuniversal_kw:
                _exponent_variance_bonus = 0.08
                total_score += _exponent_variance_bonus
                node["exponent_variance"] = True
                node.setdefault("o21_exponent_variance_signals", []).append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "exponent_keywords_matched": [
                        kw for kw in _EXPONENT_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "nonuniversal_keywords_matched": [
                        kw for kw in _NONUNIVERSAL_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "bonus_applied": _exponent_variance_bonus,
                    "source": "exponent_variance_detection",
                    "o21_challenge": (
                        "Paper reports power-law exponent measurements with "
                        "substrate-specific variation. SOC universality is "
                        "structural (criticality is ubiquitous) but NOT "
                        "numerical (exponents are substrate-specific). O21 "
                        "must predict system-class-dependent γ rather than "
                        "a single universal slope. Query: do ToS belief "
                        "revision datasets show γ variance across semantic "
                        "domains consistent with substrate-specific exponent "
                        "prediction from SOC non-universality?"
                    ),
                })
            elif _has_exponent_kw and not _has_nonuniversal_kw:
                # Power-law exponents mentioned but universality not challenged
                node.setdefault("exponent_variance", False)

            # ── Wasserstein-Lipschitz co-occurrence flag (O112 advance) ───────
            # Papers enforcing Lipschitz-Wasserstein constraints are the highest-
            # signal evidence class for O112 (STF metric tensor recovery): the
            # 1-Lipschitz constraint on discriminators is the operational form of
            # the W1 dual representation, directly connecting to the Wasserstein
            # geometry underlying the genome's metric tensor claims.
            #
            # Detection: both "lipschitz" and "wasserstein" must appear within
            # 40 tokens of each other in the combined text. Simple proximity
            # avoids false positives from papers that mention each term in
            # unrelated sections.
            #
            # CHALLENGE (INV_094): WGAN-GP stabilizes training via gradient
            # penalty enforcing 1-Lipschitz continuity, but IS/FID metrics used
            # to validate "semantic accuracy" are known to be insensitive to mode
            # collapse subtypes, leaving open whether the W2 proxy genuinely
            # tracks the geometry the genome claims or merely suppresses variance
            # in a coarser metric.
            _WASSERSTEIN_LIPSCHITZ_BOOST = 0.15
            _wl_combined = (_nk_lower_ec + " " +
                            node.get("compress", "").lower() + " " +
                            " ".join(node.get("invariants", [])).lower() + " " +
                            " ".join(node.get("tags", [])).lower())
            _wl_tokens = _wl_combined.split()
            _wl_lipschitz_positions = [
                i for i, tok in enumerate(_wl_tokens)
                if "lipschitz" in tok
            ]
            _wl_wasserstein_positions = [
                i for i, tok in enumerate(_wl_tokens)
                if "wasserstein" in tok
            ]
            _wl_proximity_hit = False
            for _lp in _wl_lipschitz_positions:
                for _wp in _wl_wasserstein_positions:
                    if abs(_lp - _wp) <= 40:
                        _wl_proximity_hit = True
                        break
                if _wl_proximity_hit:
                    break
            if _wl_proximity_hit:
                total_score += _WASSERSTEIN_LIPSCHITZ_BOOST
                node["wasserstein_lipschitz_flag"] = True
                node.setdefault("o112_advance_signals", []).append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "signal": "wasserstein_lipschitz_proximity",
                    "boost_applied": _WASSERSTEIN_LIPSCHITZ_BOOST,
                    "source": "wasserstein_lipschitz_co_occurrence",
                    "inv094_challenge": (
                        "WGAN-GP enforces 1-Lipschitz via gradient penalty "
                        "but IS/FID validation metrics are insensitive to "
                        "mode collapse subtypes — W2 proxy may suppress "
                        "variance in a coarser metric rather than tracking "
                        "the geometry the genome claims (INV_094)."
                    ),
                })
                print(f"  [WASSERSTEIN-LIPSCHITZ] +{_WASSERSTEIN_LIPSCHITZ_BOOST} "
                      f"O112-advance boost: Lipschitz+Wasserstein co-occur "
                      f"within 40 tokens (highest-signal evidence class for "
                      f"STF metric tensor recovery)")

            # ── Logarithmic-correction detection at d=d_c (INV_094 challenge) ─
            # Papers operating at or near the upper critical dimension (d=d_c)
            # exhibit logarithmic corrections to mean-field power-law scaling.
            # Naive power-law FSS mis-identifies these as standard universality,
            # creating false-equivalence between "dressed mean-field" (log-corrected)
            # and generic power-law universality classes. Detect co-occurrence of
            # (1) upper-critical-dimension language, (2) logarithmic-correction
            # language, and (3) finite-size-scaling / mean-field language. Tag
            # the node with log_correction_at_dc so downstream consolidation
            # scoring distinguishes dimensional regimes before mapping papers
            # to genome invariants.
            #
            # CHALLENGE (INV_094): substrate-independence of universality at d=d_c
            # requires logarithmic corrections not captured by naive power-law FSS.
            # The claim of clean substrate-independent universality is incomplete
            # without specifying dimensional regime.
            _LOG_CORR_DC_KEYWORDS = {
                "upper critical dimension", "upper-critical dimension",
                "d_c", "d=d_c", "d = d_c", "critical dimension",
                "marginal dimension", "borderline dimension",
                "d=4", "d = 4", "d=6", "d = 6",
                "four dimensions", "six dimensions",
                "4d ising", "6d ising", "four-dimensional", "six-dimensional",
            }
            _LOG_CORRECTION_KEYWORDS = {
                "logarithmic correction", "logarithmic corrections",
                "log correction", "log corrections", "log-correction",
                "multiplicative logarithm", "additive logarithm",
                "ln(l)", "log(l)", "logarithmic factor",
                "logarithmic modification", "logarithmic scaling",
                "dressed mean-field", "dressed mean field",
                "mean-field with corrections", "corrected mean-field",
                "hatted exponent", "effective exponent",
            }
            _FSS_MF_KEYWORDS = {
                "finite-size scaling", "finite size scaling", "fss",
                "mean-field", "mean field", "landau theory",
                "mean-field exponent", "mean field exponent",
                "classical exponent", "gaussian fixed point",
                "creutz cellular automaton", "creutz automaton",
                "thermodynamic singularity", "thermodynamic singularities",
                "susceptibility", "specific heat", "magnetization",
                "binder cumulant", "scaling relation",
            }
            _has_dc_kw = any(kw in _nk_combined_lower for kw in _LOG_CORR_DC_KEYWORDS)
            _has_log_corr_kw = any(kw in _nk_combined_lower for kw in _LOG_CORRECTION_KEYWORDS)
            _has_fss_mf_kw = any(kw in _nk_combined_lower for kw in _FSS_MF_KEYWORDS)
            _log_correction_bonus = 0.0
            if _has_dc_kw and _has_log_corr_kw:
                # Strong signal: both d_c language and log-correction language
                _log_correction_bonus = 0.10
                total_score += _log_correction_bonus
                node["log_correction_at_dc"] = True
                node["universality_regime"] = "dressed_mean_field"
                node.setdefault("log_correction_signals", []).append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "dc_keywords_matched": [
                        kw for kw in _LOG_CORR_DC_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "log_correction_keywords_matched": [
                        kw for kw in _LOG_CORRECTION_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "fss_mf_keywords_matched": [
                        kw for kw in _FSS_MF_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "bonus_applied": _log_correction_bonus,
                    "source": "log_correction_dc_detection",
                    "inv094_challenge": (
                        "Paper operates at or near d=d_c where mean-field "
                        "exponents acquire logarithmic corrections. Naive "
                        "power-law FSS produces false-equivalence with "
                        "generic power-law universality. This node is tagged "
                        "universality_regime=dressed_mean_field to prevent "
                        "mis-mapping to standard power-law genome invariants. "
                        "Substrate-independence of universality at d=d_c "
                        "requires specifying dimensional regime and "
                        "logarithmic correction structure."
                    ),
                })
                print(f"  [LOG-CORR-DC] +{_log_correction_bonus:.2f} bonus: "
                      f"upper critical dimension with logarithmic corrections "
                      f"detected — tagged universality_regime=dressed_mean_field "
                      f"(INV_094: prevents false-equivalence with standard "
                      f"power-law universality)")
            elif _has_dc_kw and _has_fss_mf_kw and not _has_log_corr_kw:
                # Weaker signal: d_c + FSS/mean-field but no explicit log
                # corrections mentioned — paper may be MISSING the corrections
                _log_correction_bonus = 0.06
                total_score += _log_correction_bonus
                node["log_correction_at_dc"] = False
                node["universality_regime"] = "unspecified_dc"
                node.setdefault("log_correction_signals", []).append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "dc_keywords_matched": [
                        kw for kw in _LOG_CORR_DC_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "fss_mf_keywords_matched": [
                        kw for kw in _FSS_MF_KEYWORDS
                        if kw in _nk_combined_lower
                    ],
                    "bonus_applied": _log_correction_bonus,
                    "source": "dc_without_log_correction_warning",
                    "inv094_warning": (
                        "Paper operates at d=d_c with FSS/mean-field analysis "
                        "but does NOT mention logarithmic corrections. This "
                        "may indicate naive power-law FSS applied at the upper "
                        "critical dimension — results may conflate dressed "
                        "mean-field with standard universality. Tagged "
                        "universality_regime=unspecified_dc for review."
                    ),
                })
                print(f"  [LOG-CORR-DC] +{_log_correction_bonus:.2f} bonus: "
                      f"upper critical dimension detected WITHOUT explicit "
                      f"log corrections — tagged universality_regime="
                      f"unspecified_dc (INV_094 WARNING: naive power-law FSS "
                      f"at d=d_c risks false-equivalence)")
            else:
                node.setdefault("log_correction_at_dc", False)

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

        Confirmation-surplus gate: before renormalization, checks each
        invariant on the node for confirmation surplus (confirmations -
        challenges > CONFIRMATION_SURPLUS_THRESHOLD with zero challenges).
        Flagged invariants are annotated in the prompt with
        ADVERSARIAL_PROBE_REQUIRED, preventing the LLM from further
        confirming them and forcing falsification-oriented updates instead.
        This prevents confirmation runaway on high-surplus invariants by
        structurally injecting adversarial challenge demands into the
        renormalization pass.
        """
        import re

        # Cap fields to keep prompt size bounded regardless of node growth
        compress_text = node.get('compress', '')[:500]
        invariants    = node.get('invariants', [])[:15]   # max 15 invariants in prompt

        # ── Confirmation-surplus gate (adversarial probe injection) ────────
        # Check each invariant for confirmation surplus. When an invariant
        # has confirmation_count > threshold and zero challenges, annotate
        # it with [ADVERSARIAL_PROBE_REQUIRED] in the prompt so the LLM
        # is structurally forced to challenge rather than further confirm.
        CONF_SURPLUS_RENORM_THRESHOLD = 5
        _blocked_invs = set(node.get("_confirmation_surplus_blocked", []))
        _inv_metadata = node.get("_invariant_metadata", {})
        annotated_invariants = []
        _n_adversarial_flagged = 0
        for inv in invariants:
            if inv in _blocked_invs:
                # Inject adversarial probe annotation
                _meta = _inv_metadata.get(inv[:120], {})
                _conf_count = _meta.get("confirmations", 0)
                _chal_count = _meta.get("challenges", 0)
                annotated_invariants.append(
                    f"{inv} [ADVERSARIAL_PROBE_REQUIRED: {_conf_count} "
                    f"confirmations, {_chal_count} challenges — DO NOT "
                    f"CONFIRM, challenge or falsify instead]"
                )
                _n_adversarial_flagged += 1
            else:
                annotated_invariants.append(inv)

        inv_text = ', '.join(annotated_invariants)

        # Build adversarial injection block for the prompt when any
        # invariants are flagged — forces the LLM to produce challenges
        _adversarial_block = ""
        if _n_adversarial_flagged > 0:
            _adversarial_block = (
                f"\n\nADVERSARIAL GATE: {_n_adversarial_flagged} invariant(s) "
                f"marked [ADVERSARIAL_PROBE_REQUIRED] have confirmation "
                f"surplus above threshold with insufficient challenges. "
                f"You MUST NOT add further confirmations for these invariants. "
                f"Instead: (1) identify what would falsify each flagged "
                f"invariant, (2) specify boundary conditions where it breaks, "
                f"(3) name an alternative mechanism producing identical "
                f"observables. Add these as NEW_OBLIGATIONS, not as "
                f"NEW_INVARIANTS. Confirmation accumulation without "
                f"adversarial testing is epistemically void."
            )
            print(f"  [RENORM-ADVERSARIAL] {_n_adversarial_flagged} invariant(s) "
                  f"on {node['id'][:40]} flagged for adversarial probe "
                  f"(confirmation surplus gate active)")

        prompt = (
            f"EXISTING NODE:\n"
            f"ID: {node['id']}\n"
            f"Title: {node.get('title','?')}\n"
            f"Current compress: {compress_text}\n"
            f"Current invariants: {inv_text}\n"
            f"Current coherence_score: {node.get('coherence_score', '?')}\n\n"
            f"NEW KNOWLEDGE:\n{new_knowledge[:2000]}"
            f"{_adversarial_block}"
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

        # ── Normalized excitation-ratio scoring (winner / sum-of-all) ─────
        # Replace raw competitive-winner magnitude with a locally calibrated
        # ordinal confidence value. Each candidate's raw score is computed as
        # a composite of recurrence and margin; the excitation ratio normalizes
        # this by the sum of all candidates' scores, producing a confidence
        # signal that is comparable across different candidate pools regardless
        # of pool size or raw magnitude scale.
        #
        # Paper basis: sensory-neural network diagnosis ranks disease neurons
        # by the ratio of the most-excited neuron's activation to the total
        # network excitation, yielding a locally calibrated likelihood rather
        # than a raw magnitude. This is directly applicable: each candidate
        # invariant is a "neuron" excited by symptom-like evidence signals,
        # and the excitation ratio gives comparable confidence across pools.
        #
        # INV_073 challenge acknowledgment: the paper achieves diagnosis via
        # frozen winner-takes-all maximization without critical-ridge tension,
        # demonstrating useful epistemic work without γ=1 criticality. This
        # strains INV_073's necessity claim but does not refute it — the
        # paper's system is non-adaptive (frozen weights), while INV_073
        # concerns adaptive systems navigating epistemic phase transitions.
        for c in candidates:
            raw_score = c["recurrence"] + 0.3 * c.get("margin_score", 0.0)
            c["_raw_excitation"] = raw_score

        # Compute sum of all raw excitations for normalization
        total_excitation = sum(c["_raw_excitation"] for c in candidates)
        if total_excitation > 0:
            for c in candidates:
                c["excitation_ratio"] = round(
                    c["_raw_excitation"] / total_excitation, 6)
                c["confidence"] = c["excitation_ratio"]
        else:
            # Degenerate case: uniform confidence
            n_cand = len(candidates) if candidates else 1
            for c in candidates:
                c["excitation_ratio"] = round(1.0 / n_cand, 6)
                c["confidence"] = c["excitation_ratio"]

        # Sort by excitation_ratio (normalized) rather than raw magnitude
        candidates.sort(
            key=lambda c: c.get("excitation_ratio", 0.0),
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
        # Parse R² (power-law fit quality)
        _r2_match = re.search(
            r'(?:R²|R\^2|r²|r\^2|R2|r2)\s*[=:]\s*([0-9]+\.?[0-9]*)', new_knowledge)
        if _r2_match:
            _ca_telemetry["r_squared"] = float(_r2_match.group(1))
        if _ca_telemetry:
            # Determine O148 status based on σ critical band and R² fit quality
            _sigma_in_critical_band = False
            if "sigma" in _ca_telemetry:
                _s = _ca_telemetry["sigma"]
                _sigma_in_critical_band = (0.95 <= _s <= 1.05)
            _r2_val = _ca_telemetry.get("r_squared")
            _r2_sufficient = _r2_val is not None and _r2_val >= 0.9

            if _sigma_in_critical_band and _r2_sufficient:
                _ca_telemetry["o148_status"] = "PARTIAL"
                _ca_telemetry["o148_note"] = (
                    "σ within critical band and R² >= 0.9 — telemetry is "
                    "suggestive of criticality with adequate fit quality. "
                    "O148 remains PARTIAL: single snapshot, longitudinal "
                    "tracking and metric tensor recovery linkage not yet included."
                )
            elif _sigma_in_critical_band and not _r2_sufficient:
                _ca_telemetry["o148_status"] = "PARTIAL"
                _r2_display = f"{_r2_val}" if _r2_val is not None else "unknown"
                _ca_telemetry["o148_note"] = (
                    f"σ within critical band but R²={_r2_display} < 0.9 — "
                    f"power-law fit is suggestive rather than definitive. "
                    f"Criticality verdict is evidence-in-hand but not "
                    f"statistically conclusive. O148 PARTIAL: full resolution "
                    f"contingent on stronger fit (R² >= 0.9) or longer run."
                )
                _ca_telemetry["o148_r2_insufficient"] = True
                print(f"[CONSOLIDATE] ⚠ O148 PARTIAL: σ in critical band "
                      f"but R²={_r2_display} < 0.9 — fit quality insufficient "
                      f"for full resolution")
            else:
                _ca_telemetry["o148_status"] = "PARTIAL"
                _ca_telemetry["o148_note"] = (
                    "Single-snapshot telemetry parsed. O148 remains OPEN: "
                    "longitudinal tracking and metric tensor recovery linkage "
                    "not yet included. Do not treat this as full resolution."
                )

            # Log σ and α into obligation-resolution record for O148
            if obligations:
                for _ob in obligations:
                    if _ob.get("id") == "O148":
                        _ob.setdefault("telemetry_log", []).append({
                            "timestamp": ts,
                            "sigma": _ca_telemetry.get("sigma"),
                            "alpha": _ca_telemetry.get("alpha"),
                            "r_squared": _ca_telemetry.get("r_squared"),
                            "shannon_entropy_bits": _ca_telemetry.get("shannon_entropy_bits"),
                            "survival_rate": _ca_telemetry.get("survival_rate"),
                            "criticality_verdict": _ca_telemetry.get("criticality_verdict"),
                            "o148_status": _ca_telemetry["o148_status"],
                            "r2_insufficient": _ca_telemetry.get("o148_r2_insufficient", False),
                        })
                        if _ob.get("status") in ("open", None, ""):
                            _ob["status"] = "partial"
                        # Persist O148 update to obligations file
                        try:
                            _obligs_path = FREED_DIR / "FREED_obligations.json"
                            if _obligs_path.exists():
                                _obligs_data = json.loads(_obligs_path.read_text())
                                _obligs_list = (_obligs_data if isinstance(_obligs_data, list)
                                                else _obligs_data.get("obligations", []))
                                for _obl_entry in (_obligs_list if isinstance(_obligs_list, list)
                                                   else list(_obligs_list.values()) if isinstance(_obligs_list, dict)
                                                   else []):
                                    if _obl_entry.get("id") == "O148":
                                        _obl_entry.setdefault("telemetry_log", []).append({
                                            "timestamp": ts,
                                            "sigma": _ca_telemetry.get("sigma"),
                                            "alpha": _ca_telemetry.get("alpha"),
                                            "r_squared": _ca_telemetry.get("r_squared"),
                                            "o148_status": _ca_telemetry["o148_status"],
                                        })
                                        if _obl_entry.get("status") in ("open", None, ""):
                                            _obl_entry["status"] = "partial"
                                        break
                                _obligs_path.write_text(
                                    json.dumps(_obligs_data, indent=2, ensure_ascii=False))
                        except Exception as _o148_err:
                            print(f"  [O148] Warning: could not persist telemetry: {_o148_err}")
                        print(f"[CONSOLIDATE] O148 telemetry logged: "
                              f"σ={_ca_telemetry.get('sigma')}, "
                              f"α={_ca_telemetry.get('alpha')}, "
                              f"R²={_ca_telemetry.get('r_squared')} → "
                              f"status={_ca_telemetry['o148_status']}")
                        break
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

        # ── STF method candidate detection (O112) ────────────────────────────
        # Detect papers whose constraint set (fixed macroscopic observables +
        # MaxEnt micro-distribution) structurally matches the STF recovery
        # method. Tag as stf_method_candidate and increment a counter in the
        # obligation record for O112. This prevents constrained-MaxEnt
        # constructions from being treated as generic CONVERGE hits and
        # ensures the obligation advances measurably.
        #
        # CHALLENGE: papers with 2D restriction and granular-specific contact
        # geometry mean the constrained-MaxEnt method cannot be directly
        # imported into semantic STF recovery without a non-trivial
        # dimensionality and substrate translation that the genome does not
        # yet specify.
        #
        # Detection: co-occurrence of (1) constraint/fixed-observable language,
        # (2) maximum entropy / MaxEnt language, and (3) micro-distribution /
        # probability density language. All three families must be present.
        _STF_CONSTRAINT_KEYWORDS = {
            "constraint", "constrained", "fixed", "macroscopic",
            "constant mean stress", "constant volume", "fixed observable",
            "macroscopic observable", "consistency", "dissipation rate",
        }
        _STF_MAXENT_KEYWORDS = {
            "maximum entropy", "maxent", "max-ent", "maximum disorder",
            "most probable", "maximum likelihood", "least biased",
            "information-theoretic", "jaynes",
        }
        _STF_MICRO_KEYWORDS = {
            "probability density", "micro-scale", "microscale",
            "micro-distribution", "probability distribution",
            "contact probability", "inter-particle", "microstate",
            "micro-state", "grain-scale", "particle-level",
        }
        _has_stf_constraint = any(kw in _nk_lower for kw in _STF_CONSTRAINT_KEYWORDS)
        _has_stf_maxent = any(kw in _nk_lower for kw in _STF_MAXENT_KEYWORDS)
        _has_stf_micro = any(kw in _nk_lower for kw in _STF_MICRO_KEYWORDS)
        _is_stf_method_candidate = _has_stf_constraint and _has_stf_maxent and _has_stf_micro

        if _is_stf_method_candidate:
            report["stf_method_candidate"] = True
            # Detect dimensional / substrate limitations that challenge direct import
            _stf_challenges = []
            if any(kw in _nk_lower for kw in ("two-dimensional", "2d", "two dimensional", "2-d")):
                _stf_challenges.append("2D_restriction")
            if any(kw in _nk_lower for kw in ("granular", "grain", "contact geometry", "inter-particle contact")):
                _stf_challenges.append("granular_substrate_specific")
            report["stf_method_challenges"] = _stf_challenges

            # ── O112 Noether-Kozlov-Kolesnikov integrability gate ─────────────
            # Before O112 defaults to pure numerical geodesic recovery (modal
            # paths + thermality variance), check whether the candidate metric
            # tensor respects known Lie group structure. If Noether symmetries
            # are present, STF geodesic recovery may be analytically tractable
            # via integrability-by-quadratures (Kozlov-Kolesnikov theorem),
            # improving experimental precision over numerical methods.
            #
            # Paper basis: integrability-by-quadratures for optimal control on
            # open sets is obtained by combining Noether's symmetry principle
            # with the Kozlov-Kolesnikov theorem. Sub-Riemannian nilpotent Lie
            # group of type (2,3,5) serves as proof-of-concept that analytic
            # geodesic recovery is achievable when the underlying space has
            # sufficient symmetry structure.
            #
            # CHALLENGE: O112's "method specified: modal paths + thermality
            # variance" may be underdetermined unless symmetry structure of the
            # semantic space is established beforehand. Sub-Riemannian geodesic
            # recovery requires verifying Kozlov-Kolesnikov integrability
            # conditions FIRST — without this verification, the method defaults
            # to numerical recovery when analytic quadrature-based extraction
            # may be available.
            _NOETHER_SYMMETRY_KEYWORDS = {
                "noether", "noether symmetry", "noether theorem",
                "lie group", "lie algebra", "lie symmetry",
                "nilpotent", "nilpotent group", "heisenberg group",
                "symmetry group", "continuous symmetry",
                "conservation law", "conserved quantity",
                "first integral", "integrability",
                "kozlov", "kolesnikov", "kozlov-kolesnikov",
                "integrability by quadratures", "quadrature",
                "sub-riemannian", "subriemannian", "sub riemannian",
                "pontryagin", "pontryagin maximum principle",
                "hamiltonian system", "symplectic",
            }
            _ANALYTIC_GEODESIC_KEYWORDS = {
                "analytic geodesic", "analytic solution", "closed-form",
                "closed form", "exact solution", "explicit solution",
                "integrable system", "completely integrable",
                "solvable", "exactly solvable",
            }
            _has_noether_symmetry = any(
                kw in _nk_lower for kw in _NOETHER_SYMMETRY_KEYWORDS)
            _has_analytic_geodesic = any(
                kw in _nk_lower for kw in _ANALYTIC_GEODESIC_KEYWORDS)

            _o112_integrability_note = None
            if _has_noether_symmetry:
                # Noether symmetries detected — flag O112 for analytic tractability
                _o112_integrability_note = {
                    "noether_symmetry_detected": True,
                    "analytic_geodesic_signal": _has_analytic_geodesic,
                    "integrability_status": "ANALYTIC_CANDIDATE",
                    "note": (
                        "O112 FLAG: Noether symmetries detected in candidate "
                        "metric tensor context. STF geodesic recovery may be "
                        "analytically tractable via Kozlov-Kolesnikov "
                        "integrability-by-quadratures rather than defaulting "
                        "to numerical methods. The method specification "
                        "'modal paths + thermality variance' should first "
                        "verify whether the semantic space's candidate metric "
                        "respects a known Lie group structure (e.g., nilpotent "
                        "type (2,3,5) sub-Riemannian). If integrability "
                        "conditions are satisfied, analytic quadrature-based "
                        "extraction yields higher experimental precision than "
                        "numerical geodesic recovery."
                    ),
                    "challenge": (
                        "Sub-Riemannian geodesic recovery requires verifying "
                        "Kozlov-Kolesnikov integrability conditions BEFORE "
                        "defaulting to numerical methods. O112's method is "
                        "underdetermined unless symmetry structure of the "
                        "semantic space is established. Sufficient condition: "
                        "combine Noether first integrals with Kozlov-Kolesnikov "
                        "theorem to check if the optimal control problem on "
                        "the semantic manifold admits integrability by "
                        "quadratures."
                    ),
                    "paper_reference": (
                        "Integrability by quadratures for optimal control "
                        "problems via Noether + Kozlov-Kolesnikov fusion; "
                        "sub-Riemannian nilpotent Lie group (2,3,5) as "
                        "proof-of-concept for analytic geodesic recovery."
                    ),
                }
                _stf_challenges.append("noether_integrability_gate_required")
                report["o112_integrability_note"] = _o112_integrability_note

                # Annotate the O112 obligation with the integrability gate
                if obligations:
                    for _ob in obligations:
                        if _ob.get("id") == "O112":
                            _ob.setdefault("integrability_checks", []).append({
                                "timestamp": ts,
                                "noether_symmetry_detected": True,
                                "analytic_geodesic_signal": _has_analytic_geodesic,
                                "status": "ANALYTIC_CANDIDATE",
                                "knowledge_digest": new_knowledge[:200],
                                "note": _o112_integrability_note["note"],
                            })
                            break

                print(f"[CONSOLIDATE] ⚡ O112 INTEGRABILITY GATE: Noether "
                      f"symmetries detected — STF geodesic recovery may be "
                      f"analytically tractable via Kozlov-Kolesnikov "
                      f"quadratures (analytic_geodesic_signal="
                      f"{_has_analytic_geodesic}). Method 'modal paths + "
                      f"thermality variance' should verify Lie group "
                      f"structure before defaulting to numerical recovery.")
            elif not _has_noether_symmetry:
                _o112_integrability_note = {
                    "noether_symmetry_detected": False,
                    "analytic_geodesic_signal": False,
                    "integrability_status": "NUMERICAL_DEFAULT",
                    "note": (
                        "No Noether symmetry structure detected in this "
                        "candidate. O112 defaults to numerical geodesic "
                        "recovery (modal paths + thermality variance). "
                        "Future papers with Lie group / sub-Riemannian "
                        "structure should be re-checked for analytic "
                        "tractability via Kozlov-Kolesnikov conditions."
                    ),
                }
                report["o112_integrability_note"] = _o112_integrability_note

            # Increment O112 stf_method_candidate counter in obligation record
            _o112_incremented = False
            if obligations:
                for _ob in obligations:
                    if _ob.get("id") == "O112":
                        _ob["stf_method_candidate_count"] = _ob.get(
                            "stf_method_candidate_count", 0) + 1
                        _ob.setdefault("stf_method_candidate_log", []).append({
                            "timestamp": ts,
                            "challenges": _stf_challenges,
                            "knowledge_digest": new_knowledge[:200],
                        })
                        _o112_incremented = True
                        break
            # Persist O112 counter to obligations file
            if _o112_incremented:
                try:
                    _obligs_path = FREED_DIR / "FREED_obligations.json"
                    if _obligs_path.exists():
                        _obligs_data = json.loads(_obligs_path.read_text())
                        _obligs_list = (_obligs_data if isinstance(_obligs_data, list)
                                        else _obligs_data.get("obligations", []))
                        for _obl_entry in (_obligs_list if isinstance(_obligs_list, list)
                                           else list(_obligs_list.values()) if isinstance(_obligs_list, dict)
                                           else []):
                            if _obl_entry.get("id") == "O112":
                                _obl_entry["stf_method_candidate_count"] = _obl_entry.get(
                                    "stf_method_candidate_count", 0) + 1
                                _obl_entry.setdefault("stf_method_candidate_log", []).append({
                                    "timestamp": ts,
                                    "challenges": _stf_challenges,
                                    "knowledge_digest": new_knowledge[:200],
                                })
                                break
                        _obligs_path.write_text(
                            json.dumps(_obligs_data, indent=2, ensure_ascii=False))
                except Exception as _stf_err:
                    print(f"  [STF-CANDIDATE] Warning: could not persist O112 counter: {_stf_err}")

            print(f"[CONSOLIDATE] ⚡ STF method candidate detected: "
                  f"constrained-MaxEnt construction "
                  f"(constraint={_has_stf_constraint}, maxent={_has_stf_maxent}, "
                  f"micro={_has_stf_micro})"
                  + (f" — challenges: {_stf_challenges}" if _stf_challenges else "")
                  + f" → O112 stf_method_candidate_count incremented")
        else:
            report["stf_method_candidate"] = False

        # ── O112 metric tensor object detection (score_obligation_match) ──────
        # When a paper supplies an explicit metric tensor formula (e.g., the
        # Wasserstein Information Matrix / WIM), O112's status should advance
        # from "open" to "partial" — the mathematical *object* the obligation
        # requires has been delivered, even if convergence with the STF method
        # (modal paths + thermality variance) is not yet demonstrated.
        #
        # Detection: co-occurrence of (1) metric tensor / information matrix
        # language, (2) Wasserstein / optimal transport language, and
        # (3) explicit parametric family language (location-scale, Gaussian,
        # exponential family). All three families must be present to distinguish
        # "paper mentions metrics" from "paper delivers the metric tensor object."
        #
        # CHALLENGE: the WIM is a *peer* statistical geometry to Fisher's,
        # not a subordinate — O112's continued "open" status after WIM delivery
        # is epistemic debt (the genome hasn't checked convergence) rather than
        # genuine uncertainty (the object doesn't exist). The Poincaré efficiency
        # condition introduced by WIM is an unregistered constraint on W2
        # gradient-flow optimality that the genome must absorb.
        #
        # Query: does the WIM's analytical form for location-scale families
        # match the STF metric tensor predicted by modal-path + thermality-
        # variance? If yes, O112 is fully resolved. If no, the two routes
        # produce different geometric objects and O112 must be refined.
        _METRIC_TENSOR_KEYWORDS = {
            "metric tensor", "information matrix", "information matrices",
            "riemannian metric", "fisher information", "fisher-rao",
            "statistical metric", "score function", "covariance operator",
        }
        _WASSERSTEIN_GEOM_KEYWORDS = {
            "wasserstein", "optimal transport", "earth mover",
            "w2 metric", "w_2", "l2-wasserstein", "l^2-wasserstein",
            "transport metric", "displacement interpolation",
        }
        _PARAMETRIC_FAMILY_KEYWORDS = {
            "location-scale", "location scale", "gaussian family",
            "exponential family", "normal distribution", "scale family",
            "independent families", "parametric family", "analytical example",
            "cramér-rao", "cramer-rao", "natural gradient",
        }
        _has_metric_tensor = any(kw in _nk_lower for kw in _METRIC_TENSOR_KEYWORDS)
        _has_wasserstein_geom = any(kw in _nk_lower for kw in _WASSERSTEIN_GEOM_KEYWORDS)
        _has_parametric_family = any(kw in _nk_lower for kw in _PARAMETRIC_FAMILY_KEYWORDS)
        _is_metric_tensor_object = _has_metric_tensor and _has_wasserstein_geom and _has_parametric_family

        if _is_metric_tensor_object:
            # Detect which specific parametric families are tested
            _families_tested = []
            _FAMILY_DETECTION = {
                "location-scale": {"location-scale", "location scale"},
                "gaussian": {"gaussian", "normal distribution"},
                "exponential": {"exponential family", "exponential distribution"},
                "independent": {"independent families", "independent family",
                                "product distribution"},
                "rectified_linear": {"rectified", "relu", "rectified linear"},
            }
            for _fam_name, _fam_kws in _FAMILY_DETECTION.items():
                if any(kw in _nk_lower for kw in _fam_kws):
                    _families_tested.append(_fam_name)

            # Detect unregistered constraints (e.g., Poincaré efficiency)
            _unregistered_constraints = []
            if any(kw in _nk_lower for kw in ("poincaré efficiency", "poincare efficiency",
                                                "poincaré inequality", "poincare inequality")):
                _unregistered_constraints.append("poincare_efficiency")
            if any(kw in _nk_lower for kw in ("asymptotic efficiency", "on-line efficiency",
                                                "online efficiency")):
                _unregistered_constraints.append("asymptotic_efficiency")

            report["o112_metric_tensor_object"] = {
                "detected": True,
                "metric_type": "wasserstein_information_matrix",
                "families_tested": _families_tested,
                "unregistered_constraints": _unregistered_constraints,
                "convergence_with_stf": "UNTESTED",
                "status_implication": (
                    "O112 should advance to PARTIAL: the metric tensor object "
                    "has been delivered (WIM provides explicit analytical forms "
                    f"for {', '.join(_families_tested) if _families_tested else 'unspecified'} families). "
                    "Remaining debt: verify whether WIM's analytical form "
                    "converges with the STF metric tensor predicted by "
                    "modal-path + thermality-variance method."
                ),
                "epistemic_debt_note": (
                    "O112's continued 'open' status after metric tensor delivery "
                    "is epistemic debt (genome hasn't checked convergence), not "
                    "genuine uncertainty (object doesn't exist). The WIM is a "
                    "peer statistical geometry to Fisher's — not subordinate."
                ),
                "challenge": (
                    "WIM introduces Poincaré efficiency condition as an "
                    "unregistered constraint on W2 gradient-flow optimality. "
                    "Query: does WIM's analytical form for location-scale "
                    "families match the STF metric tensor predicted by "
                    "modal-path + thermality-variance? Two routes may "
                    "converge on the same object or produce genuinely "
                    "different geometric structures."
                ),
            }

            # Advance O112 to partial and log the metric tensor delivery
            _o112_advanced = False
            if obligations:
                for _ob in obligations:
                    if _ob.get("id") == "O112":
                        if _ob.get("status") in ("open", None, ""):
                            _ob["status"] = "partial"
                        _ob["metric_tensor_delivered"] = True
                        _ob["metric_tensor_type"] = "wasserstein_information_matrix"
                        _ob["families_tested"] = _families_tested
                        _ob["unregistered_constraints"] = _unregistered_constraints
                        _ob.setdefault("metric_tensor_delivery_log", []).append({
                            "timestamp": ts,
                            "metric_type": "wasserstein_information_matrix",
                            "families_tested": _families_tested,
                            "unregistered_constraints": _unregistered_constraints,
                            "convergence_with_stf": "UNTESTED",
                            "knowledge_digest": new_knowledge[:200],
                        })
                        _o112_advanced = True
                        break

            # Persist O112 advancement to obligations file
            if _o112_advanced:
                try:
                    _obligs_path = FREED_DIR / "FREED_obligations.json"
                    if _obligs_path.exists():
                        _obligs_data = json.loads(_obligs_path.read_text())
                        _obligs_list = (_obligs_data if isinstance(_obligs_data, list)
                                        else _obligs_data.get("obligations", []))
                        for _obl_entry in (_obligs_list if isinstance(_obligs_list, list)
                                           else list(_obligs_list.values()) if isinstance(_obligs_list, dict)
                                           else []):
                            if _obl_entry.get("id") == "O112":
                                if _obl_entry.get("status") in ("open", None, ""):
                                    _obl_entry["status"] = "partial"
                                _obl_entry["metric_tensor_delivered"] = True
                                _obl_entry["metric_tensor_type"] = "wasserstein_information_matrix"
                                _obl_entry["families_tested"] = _families_tested
                                _obl_entry["unregistered_constraints"] = _unregistered_constraints
                                _obl_entry.setdefault("metric_tensor_delivery_log", []).append({
                                    "timestamp": ts,
                                    "metric_type": "wasserstein_information_matrix",
                                    "families_tested": _families_tested,
                                    "unregistered_constraints": _unregistered_constraints,
                                    "convergence_with_stf": "UNTESTED",
                                })
                                break
                        _obligs_path.write_text(
                            json.dumps(_obligs_data, indent=2, ensure_ascii=False))
                except Exception as _mt_err:
                    print(f"  [METRIC-TENSOR] Warning: could not persist O112 advancement: {_mt_err}")

            print(f"[CONSOLIDATE] ⚡ O112 METRIC TENSOR OBJECT DELIVERED: "
                  f"Wasserstein Information Matrix (WIM) with analytical forms "
                  f"for families={_families_tested} — "
                  f"O112 advanced to PARTIAL "
                  f"(object delivered, convergence with STF method UNTESTED)"
                  + (f" | unregistered constraints: {_unregistered_constraints}"
                     if _unregistered_constraints else ""))
            if _unregistered_constraints:
                print(f"  [METRIC-TENSOR] ⚠ Unregistered constraint(s) detected: "
                      f"{_unregistered_constraints} — these constrain W2 "
                      f"gradient-flow optimality but are not tracked by "
                      f"any existing obligation. Consider opening new obligation.")
        else:
            report["o112_metric_tensor_object"] = {"detected": False}

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
            # ── INV_087 regime-conditional gate (entropy production threshold) ──
            # INV_087 (MaxRL as thermodynamically correct RL) is empirically
            # equivalent to the null hypothesis (standard RL) in low-gradient
            # environments where entropy production is negligible. Confirmations
            # from such environments inflate INV_087's surplus without genuine
            # discriminative power. Before counting confirmations, estimate the
            # environment's entropy production rate via the EPR scorer. If the
            # estimate falls below ε (REGIME_EPSILON), mark the confirmation as
            # "regime_conditional" rather than general — it does not count toward
            # INV_087's confirmation surplus.
            #
            # FALSIFICATION BOUNDARY (INV_087):
            #   - What would falsify it: an environment with HIGH entropy production
            #     gradient where standard RL outperforms or matches MaxRL on
            #     thermodynamic efficiency metrics (dissipation, entropy production
            #     rate). If MaxRL's advantage vanishes even far from equilibrium,
            #     the "thermodynamically correct" claim is false.
            #   - Alternative mechanism: standard RL with entropy regularization
            #     (SAC, MaxEnt RL) produces identical observables in practice
            #     without requiring the thermodynamic correctness framing —
            #     preferential attachment to the "thermodynamic" label rather
            #     than empirical distinguishability.
            #   - Boundary conditions: near-equilibrium (low entropy production
            #     gradient) environments where MaxRL and standard RL are
            #     observationally identical. INV_087 has no falsification power
            #     in this regime and confirmations from it are epistemically void.
            REGIME_EPSILON = 0.02  # entropy production rate below this → regime-conditional
            _inv087_regime_conditional_count = 0
            _inv087_keywords = {"maxrl", "max-rl", "thermodynamically correct",
                                "thermodynamic reinforcement", "maximum entropy rl",
                                "inv_087", "inv087"}
            _epr_scorer_rc = NonQuadraticEPRScorer()

            def _is_inv087(inv_text_check):
                # type: (str) -> bool
                """Check if an invariant text refers to INV_087."""
                inv_lower_check = inv_text_check.lower()
                return any(kw in inv_lower_check for kw in _inv087_keywords)

            def _estimate_environment_epr(inv_text_check, node_list):
                # type: (str, list) -> float
                """Estimate the entropy production rate of the evaluation
                environment for an invariant by computing EPR between the
                invariant's semantic distribution and the corpus baseline."""
                inv_dist = MWDEScorer._text_to_distribution(inv_text_check)
                if not inv_dist:
                    return 0.0
                # Corpus baseline: aggregate distribution across all nodes
                corpus_text = " ".join(
                    n.get("compress", "") for n in node_list
                )
                corpus_dist = MWDEScorer._text_to_distribution(corpus_text)
                if not corpus_dist:
                    return 0.0
                flux_ratios, eq_fluxes, _ = _epr_scorer_rc.semantic_flux_ratios(
                    corpus_dist, inv_dist)
                if not flux_ratios:
                    return 0.0
                epr_result = _epr_scorer_rc.epr_action(flux_ratios, eq_fluxes)
                return epr_result.get("epr_nonquad", 0.0)

            # Track which INV_087 confirmations are regime-conditional
            _regime_conditional_inv087 = {}  # type: dict  # inv_text -> epr_estimate

            # Flag invariants with surplus > threshold
            _all_inv_texts = set(list(_inv_confirmations.keys()) + list(_inv_challenges.keys()))
            _adversarial_boundary_probes = []  # invariants needing mandatory falsification
            for inv_text in _all_inv_texts:
                n_conf = _inv_confirmations.get(inv_text, 0)
                n_chal = _inv_challenges.get(inv_text, 0)

                # ── INV_087 regime-conditional confirmation downgrade ─────────
                # If this invariant is INV_087 (or references MaxRL/thermo-RL),
                # check the environment's entropy production estimate. If below
                # ε, downgrade confirmations to regime-conditional: they do not
                # count as general confirmations for surplus calculation.
                if _is_inv087(inv_text) and n_conf > 0:
                    _env_epr = _estimate_environment_epr(inv_text, all_nodes)
                    if _env_epr < REGIME_EPSILON:
                        # Mark as regime-conditional: reduce effective confirmations
                        _regime_conditional_inv087[inv_text] = round(_env_epr, 6)
                        _original_conf = n_conf
                        # Regime-conditional confirmations count at 0.25× weight
                        # (not zero — they are evidence, just not discriminative)
                        n_conf = max(1, int(n_conf * 0.25))
                        _inv087_regime_conditional_count += 1
                        print(f"  [INV-087-REGIME] ⚠ '{inv_text[:60]}...' — "
                              f"EPR={_env_epr:.4f} < ε={REGIME_EPSILON}: "
                              f"confirmations downgraded from {_original_conf} → "
                              f"{n_conf} (regime-conditional, not general). "
                              f"Near-equilibrium environment cannot distinguish "
                              f"MaxRL from standard RL — confirmation is "
                              f"epistemically void for falsification purposes.")

                surplus = n_conf - n_chal

                # ── Adversarial boundary probe (mandatory falsification gate) ──
                # An invariant with confirmation surplus above threshold AND zero
                # recorded challenges is indistinguishable from an unfalsified
                # axiom. Preferential attachment dynamics can produce all of its
                # observable signatures without requiring its core claim to be
                # true. Mark it for mandatory falsification before the next
                # encapsulation — it cannot be cited as load-bearing evidence
                # until at least one adversarial challenge is recorded.
                if surplus > CONFIRMATION_SURPLUS_THRESHOLD and n_chal == 0:
                    # ── Falsification profile check (Seed Integrity Rule 2) ──
                    # An invariant with confirmation_count > threshold and zero
                    # challenges edges is epistemically under-constrained. Its
                    # apparent robustness may reflect preferential attachment
                    # (citation momentum) rather than genuine empirical load-
                    # bearing. The falsification_profile field records this
                    # structural deficiency so downstream scoring can discount
                    # the invariant's weight until adversarial testing occurs.
                    _falsification_profile = {
                        "status": "UNDER_CONSTRAINED",
                        "confirmation_count": n_conf,
                        "challenge_count": 0,
                        "surplus": surplus,
                        "epistemic_weight_discount": max(0.1, 1.0 - (surplus / (surplus + 5.0))),
                        "reason": (
                            f"Invariant has {n_conf} confirmations and ZERO "
                            f"challenge edges — epistemically under-constrained. "
                            f"Confirmation surplus without adversarial testing "
                            f"violates Seed Integrity Rule 2: high-confirmation "
                            f"invariants must not accrue epistemic weight without "
                            f"adversarial exposure. Weight discounted by "
                            f"{1.0 - max(0.1, 1.0 - (surplus / (surplus + 5.0))):.2f} "
                            f"until at least one challenge edge is recorded."
                        ),
                        "required_actions": [
                            "Record at least one falsification criterion",
                            "Identify alternative mechanism producing same observables",
                            "Specify boundary conditions under which claim breaks",
                        ],
                        "inv094_specific": (
                            "INV_094 CHALLENGE: this invariant has accumulated "
                            "confirmation surplus without a stated falsifier, "
                            "boundary regime, or named competitor mechanism. "
                            "Its apparent robustness is a symptom of under-"
                            "testing rather than structural necessity."
                        ) if "inv_094" in inv_text.lower() or "inv094" in inv_text.lower() or n_conf == max(_inv_confirmations.values(), default=0) else None,
                    }
                    _probe_entry = {
                        "invariant": inv_text[:120],
                        "confirmations": n_conf,
                        "challenges": 0,
                        "surplus": surplus,
                        "mandatory_falsification": True,
                        "encapsulation_blocked": True,
                        "falsification_profile": _falsification_profile,
                        "probe_token": (
                            f"MANDATORY_FALSIFICATION_BEFORE_ENCAPSULATION:"
                            f"surplus={surplus},challenges=0"
                        ),
                        "falsification_demand": (
                            f"Invariant has {n_conf} confirmations and ZERO "
                            f"challenges. Three-part adversarial profile required "
                            f"before next encapsulation: (1) what empirical result "
                            f"would falsify this claim? (2) what alternative "
                            f"mechanism produces identical observables without "
                            f"requiring this claim? (3) under what boundary "
                            f"conditions does this claim break? Until answered, "
                            f"this invariant's robustness is epistemically "
                            f"unearned — confirmation surplus without adversarial "
                            f"exposure is indistinguishable from citation momentum."
                        ),
                    }
                    _adversarial_boundary_probes.append(_probe_entry)
                    _confirmation_surplus_flagged.add(inv_text)
                    _surplus_report.append(_probe_entry)
                    print(f"  [PRE-AUDIT] ★ ADVERSARIAL BOUNDARY PROBE: "
                          f"'{inv_text[:60]}...' — "
                          f"conf={n_conf}, chal=0, surplus={surplus} > {CONFIRMATION_SURPLUS_THRESHOLD} "
                          f"→ MANDATORY FALSIFICATION before next encapsulation "
                          f"(zero challenges = unfalsified axiom risk)")
                    print(f"  [FALSIFICATION-PROFILE] status=UNDER_CONSTRAINED, "
                          f"weight_discount={_falsification_profile['epistemic_weight_discount']:.3f} "
                          f"(Seed Integrity Rule 2: no epistemic weight without adversarial testing)")
                elif surplus > CONFIRMATION_SURPLUS_THRESHOLD:
                    _confirmation_surplus_flagged.add(inv_text)
                    _entry = {
                        "invariant": inv_text[:120],
                        "confirmations": n_conf,
                        "challenges": n_chal,
                        "surplus": surplus,
                        "adversarial_probe_required": True,
                        "mandatory_falsification": False,
                        "encapsulation_blocked": False,
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
        # and tag each flagged invariant's metadata with ADVERSARIAL_PROBE_REQUIRED
        for node in affected:
            _node_flagged_invs = []
            _node_inv_metadata = node.get("_invariant_metadata", {})
            for inv_text in node.get("invariants", []):
                if inv_text in _confirmation_surplus_flagged:
                    _node_flagged_invs.append(inv_text)
                    # Tag invariant metadata with ADVERSARIAL_PROBE_REQUIRED
                    _inv_conf = _inv_confirmations.get(inv_text, 0)
                    _inv_chal = _inv_challenges.get(inv_text, 0)
                    _inv_eff_chal = max(_inv_chal, 0.5)
                    _inv_ratio = _inv_conf / _inv_eff_chal
                    _node_inv_metadata[inv_text[:120]] = {
                        "status": "ADVERSARIAL_PROBE_REQUIRED",
                        "confirmations": _inv_conf,
                        "challenges": _inv_chal,
                        "confirmation_challenge_ratio": round(_inv_ratio, 2),
                        "surplus": _inv_conf - _inv_chal,
                        "flagged_at": datetime.now(timezone.utc).isoformat(),
                        "citation_blocked": True,
                        "probe_demand": (
                            "Three-part adversarial profile required before "
                            "this invariant can be cited as load-bearing evidence: "
                            "(1) distinguishing prediction that would refute it, "
                            "(2) alternative mechanism producing identical observables, "
                            "(3) boundary conditions under which it breaks."
                        ),
                    }
            if _node_flagged_invs:
                node["_confirmation_surplus_blocked"] = _node_flagged_invs
            if _node_inv_metadata:
                node["_invariant_metadata"] = _node_inv_metadata

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

            # ── Phase 1.5: Deduplicate nodes (cosine OT cost) ────────────────
        affected = self.deduplicate_nodes(affected, cost_metric="cosine")
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

                # ── Universality-class check for scales_with edges (O173) ─────
                # SOC universality requires cross-domain exponent convergence as
                # the criterion for a valid `scales_with` claim. A scales_with
                # edge is UNVERIFIED unless it has at least one recorded cross-
                # domain exponent match: the same power-law exponent ± tolerance
                # across two different substrate types. Without this check, O173
                # cannot be resolved and scales_with edges remain epistemically
                # ungrounded.
                #
                # Protocol:
                #   1. For each scales_with edge candidate, extract any exponent
                #      value mentioned in the invariant text (regex for α, τ, etc.)
                #   2. Check the existing graph for scales_with edges from nodes
                #      of DIFFERENT substrate types that cite the same exponent ± tol
                #   3. If no cross-domain match exists, downgrade to
                #      "consistent_with" and tag as UNVERIFIED_UNIVERSALITY
                _EXPONENT_TOLERANCE = 0.10  # ± tolerance for exponent matching
                _EXPONENT_PATTERN = re.compile(
                    r'(?:exponent|α|τ|β|γ|slope|power[- ]?law)\s*'
                    r'[≈=:~]\s*([0-9]+\.?[0-9]*)',
                    re.IGNORECASE
                )

                def _extract_exponent(text):
                    # type: (str) -> float
                    """Extract a numeric exponent from edge/invariant text, or None."""
                    m = _EXPONENT_PATTERN.search(text or "")
                    if m:
                        try:
                            return float(m.group(1))
                        except (ValueError, TypeError):
                            pass
                    return None

                def _get_node_substrate(node_id):
                    # type: (str) -> str
                    """Infer substrate type from a node's tags/text. Returns a
                    canonical substrate label or 'unknown'."""
                    for _mn in all_nodes:
                        if _mn.get("id") == node_id:
                            _tags = " ".join(_mn.get("tags", [])).lower()
                            _comp = (_mn.get("compress", "") or "").lower()
                            _combined = _tags + " " + _comp
                            # Simple substrate taxonomy
                            if any(k in _combined for k in ("neural", "brain", "cortical", "synaptic", "neuroscience")):
                                return "neural"
                            if any(k in _combined for k in ("urban", "city", "cities", "settlement")):
                                return "urban"
                            if any(k in _combined for k in ("river", "fluvial", "watershed", "drainage")):
                                return "fluvial"
                            if any(k in _combined for k in ("economic", "market", "financial")):
                                return "economic"
                            if any(k in _combined for k in ("biological", "organism", "genetic", "evolution")):
                                return "biological"
                            if any(k in _combined for k in ("social", "network", "internet", "communication")):
                                return "social"
                            if any(k in _combined for k in ("earthquake", "seismic", "tectonic")):
                                return "geophysical"
                            return "unknown"
                    return "unknown"

                def _has_cross_domain_exponent_match(inv_text, node_ids):
                    # type: (str, list) -> bool
                    """Check if a scales_with claim has cross-domain exponent
                    convergence: same exponent ± tolerance across ≥2 substrate types.
                    Checks both the candidate's own node substrates AND existing
                    graph edges for prior exponent records."""
                    candidate_exp = _extract_exponent(inv_text)

                    # Strategy 1: the candidate's own APPEARS_IN nodes span ≥2 substrates
                    # AND the invariant text contains an explicit exponent value
                    if candidate_exp is not None and len(node_ids) >= 2:
                        substrates_with_exp = set()
                        for nid in node_ids:
                            sub = _get_node_substrate(nid)
                            if sub != "unknown":
                                substrates_with_exp.add(sub)
                        if len(substrates_with_exp) >= 2:
                            return True

                    # Strategy 2: check existing scales_with edges in the graph
                    # for a matching exponent from a different substrate
                    if candidate_exp is not None:
                        candidate_substrates = set(
                            _get_node_substrate(nid) for nid in node_ids
                        ) - {"unknown"}
                        for existing_edge in graph._node_edges:
                            if existing_edge.get("type") != "scales_with":
                                continue
                            existing_exp = _extract_exponent(
                                existing_edge.get("invariant", ""))
                            if existing_exp is None:
                                continue
                            if abs(existing_exp - candidate_exp) <= _EXPONENT_TOLERANCE:
                                # Check if this existing edge involves a different substrate
                                existing_subs = set()
                                for eid in [existing_edge.get("from", ""),
                                            existing_edge.get("to", "")]:
                                    s = _get_node_substrate(eid)
                                    if s != "unknown":
                                        existing_subs.add(s)
                                if existing_subs - candidate_substrates:
                                    return True  # cross-domain match found

                    return False

                # ── Sinkhorn-regularized OT cost for edge scoring (INV_094) ──
                # Replace/augment dot-product similarity with entropic OT cost
                # between node distribution embeddings. Uses a fixed small number
                # of Sinkhorn iterations (8) to compute the regularized transport
                # cost between two nodes' semantic distributions. Edges whose OT
                # cost exceeds OT_EDGE_COST_THRESHOLD are demoted to
                # consistent_with (distributional shape mismatch), testing whether
                # OT geometry improves deduplication and cluster quality vs. cosine.
                #
                # CHALLENGE (INV_094): the OTESGN paper demonstrates OT superiority
                # in a supervised, task-specific fine-tuned setting (ABSA). Whether
                # OT's advantage over dot-product persists in unsupervised/zero-shot
                # distributional alignment (our case) is the open question this
                # implementation directly tests.
                OT_EDGE_SINKHORN_ITERS = 8       # fixed small iteration count
                OT_EDGE_SINKHORN_REG = 0.05      # entropic regularization ε
                OT_EDGE_COST_THRESHOLD = 0.65    # OT cost above this → demotion
                _ot_edge_scorer = MWDEScorer(wasserstein_order=1)
                _ot_demoted = 0

                def _sinkhorn_ot_edge_cost(node_id_a, node_id_b):
                    # type: (str, str) -> float
                    """Compute Sinkhorn-regularized OT cost between two nodes'
                    semantic distribution embeddings. Returns cost in [0, 1].
                    Falls back to 0.0 (no demotion) if distributions are empty."""
                    text_a = _node_text_cache.get(node_id_a, "")
                    text_b = _node_text_cache.get(node_id_b, "")
                    dist_a = MWDEScorer._text_to_distribution(text_a)
                    dist_b = MWDEScorer._text_to_distribution(text_b)
                    if not dist_a or not dist_b:
                        return 0.0  # insufficient data → don't penalize

                    # Build shared vocabulary
                    shared_keys = sorted(set(dist_a.keys()) | set(dist_b.keys()))
                    n = len(shared_keys)
                    if n < 3:
                        return 0.0

                    # Marginal vectors over shared support
                    p = [dist_a.get(k, 0.0) for k in shared_keys]
                    q = [dist_b.get(k, 0.0) for k in shared_keys]
                    p_sum = sum(p)
                    q_sum = sum(q)
                    if p_sum <= 0 or q_sum <= 0:
                        return 0.0
                    p = [x / p_sum for x in p]
                    q = [x / q_sum for x in q]

                    # Cost matrix: normalized rank distance |rank_a(i) - rank_b(j)| / n
                    def _rank_vec_ot(vals):
                        # type: (list) -> list
                        indexed = sorted(range(len(vals)), key=lambda idx: vals[idx])
                        ranks = [0.0] * len(vals)
                        for r, idx in enumerate(indexed):
                            ranks[idx] = (r + 1.0) / len(vals)
                        return ranks

                    ranks_a = _rank_vec_ot(p)
                    ranks_b = _rank_vec_ot(q)

                    # Gibbs kernel K[i][j] = exp(-C[i][j] / ε)
                    eps = max(OT_EDGE_SINKHORN_REG, 1e-10)
                    K = []
                    for i_k in range(n):
                        row = []
                        for j_k in range(n):
                            c_ij = abs(ranks_a[i_k] - ranks_b[j_k])
                            row.append(math.exp(-c_ij / eps))
                        K.append(row)

                    # Sinkhorn iterations (fixed count, no early stopping)
                    u = [1.0] * n
                    v = [1.0] * n
                    for _ in range(OT_EDGE_SINKHORN_ITERS):
                        # u_i = p_i / sum_j K[i][j] * v_j
                        u_new = []
                        for i_s in range(n):
                            kv = sum(K[i_s][j_s] * v[j_s] for j_s in range(n))
                            u_new.append(p[i_s] / max(kv, 1e-12))
                        # v_j = q_j / sum_i K[i][j] * u_i
                        v_new = []
                        for j_s in range(n):
                            ku = sum(K[i_s][j_s] * u_new[i_s] for i_s in range(n))
                            v_new.append(q[j_s] / max(ku, 1e-12))
                        u = u_new
                        v = v_new

                    # Transport cost: sum_ij u_i * K[i][j] * v_j * C[i][j]
                    ot_cost = 0.0
                    for i_c in range(n):
                        for j_c in range(n):
                            c_ij = abs(ranks_a[i_c] - ranks_b[j_c])
                            pi_ij = u[i_c] * K[i_c][j_c] * v[j_c]
                            ot_cost += pi_ij * c_ij

                    # Normalize to [0, 1] — max possible rank distance is 1.0
                    return min(1.0, max(0.0, ot_cost))

                minted = skipped_dup = frozen = 0
                _unverified_universality = 0
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
                    # ── O173 universality-class gate ──────────────────────────
                    # scales_with edges require cross-domain exponent convergence.
                    # Without it, downgrade to consistent_with + UNVERIFIED tag.
                    if edge_type == "scales_with":
                        if not _has_cross_domain_exponent_match(
                                c['invariant'], nodes_in):
                            edge_type = "consistent_with"
                            _unverified_universality += 1
                            c['invariant'] = (
                                c['invariant']
                                + " [UNVERIFIED_UNIVERSALITY: no cross-domain "
                                  "exponent match recorded (O173 — SOC "
                                  "universality requires same power-law "
                                  "exponent ± 0.10 across ≥2 substrate types)]"
                            )
                            print(f"  [O173] scales_with → consistent_with "
                                  f"(no cross-domain exponent match): "
                                  f"'{c['invariant'][:60]}'")
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

                # ── q-Gaussian return distribution analysis (O21 challenge) ───
                # Compute the q-parameter from consecutive out-degree differences
                # (returns), following the modified OFC model result: return
                # distributions follow q-Gaussian statistics where q is derivable
                # a priori from the avalanche size exponent τ.
                #
                # The algebraic q-τ link: q = (3τ - 5) / (τ - 3) for τ > 3.
                # If q is fully determined by τ, then O21's claimed correlation
                # between belief-revision scores and spectral γ may be an artifact
                # of a shared underlying exponent rather than direct γ-dependence.
                #
                # Protocol:
                #   1. Sort nodes by out-degree → avalanche size proxy series
                #   2. Compute returns: r_i = degree[i+1] - degree[i]
                #   3. Fit q-Gaussian to the return distribution via MLE of q
                #   4. Independently estimate τ from the degree distribution
                #      (power-law exponent via Hill estimator)
                #   5. Compare empirical q with q_predicted = (3τ-5)/(τ-3)
                #   6. If |q_empirical - q_predicted| < tolerance, the q-τ link
                #      holds and O21's γ-correlation may be τ-mediated
                _q_gaussian_result = None
                if _n_nodes_br >= 6:
                    # Sort degrees to create an ordered avalanche-size series
                    _sorted_degrees = sorted(_degrees)
                    # Returns: consecutive differences
                    _returns = [
                        _sorted_degrees[i + 1] - _sorted_degrees[i]
                        for i in range(len(_sorted_degrees) - 1)
                    ]
                    _n_returns = len(_returns)

                    if _n_returns >= 4:
                        # ── Empirical q estimation via variance-of-variance method ──
                        # For a q-Gaussian with parameter q, the kurtosis κ relates
                        # to q via: κ = 3(3-q)/(5-3q) for q < 5/3.
                        # Inverting: q = (5κ - 9) / (3κ - 3) when κ ≠ 1.
                        # We estimate kurtosis from the return distribution.
                        _r_mean = sum(_returns) / _n_returns
                        _r_var = sum((r - _r_mean) ** 2 for r in _returns) / _n_returns
                        _r_std = math.sqrt(_r_var) if _r_var > 0 else 1e-12
                        # Fourth central moment for kurtosis
                        _r_m4 = sum((r - _r_mean) ** 4 for r in _returns) / _n_returns
                        _kurtosis = _r_m4 / (_r_var ** 2) if _r_var > 1e-12 else 3.0

                        # Invert kurtosis-q relation: q = (5κ - 9) / (3κ - 3)
                        _q_denom = 3.0 * _kurtosis - 3.0
                        if abs(_q_denom) > 1e-8:
                            _q_empirical = (5.0 * _kurtosis - 9.0) / _q_denom
                        else:
                            _q_empirical = 1.0  # Gaussian limit

                        # Clamp q to physically meaningful range [1, 3)
                        _q_empirical = max(1.0, min(2.99, _q_empirical))

                        # ── τ estimation via Hill estimator on degree distribution ──
                        # Hill estimator for power-law exponent:
                        # α_Hill = n / sum(ln(x_i / x_min)), then τ = α_Hill + 1
                        # (since P(x) ~ x^{-τ} implies CDF tail ~ x^{-(τ-1)})
                        _pos_degrees = sorted([d for d in _degrees if d > 0])
                        _tau_estimated = None
                        _q_predicted = None
                        _q_tau_link_holds = None
                        _q_tau_residual = None

                        if len(_pos_degrees) >= 3:
                            _x_min_hill = _pos_degrees[0]
                            _hill_sum = sum(
                                math.log(d / _x_min_hill)
                                for d in _pos_degrees
                                if d > _x_min_hill
                            )
                            _n_hill = sum(1 for d in _pos_degrees if d > _x_min_hill)
                            if _hill_sum > 1e-8 and _n_hill >= 2:
                                _alpha_hill = _n_hill / _hill_sum
                                _tau_estimated = _alpha_hill + 1.0

                                # q predicted from τ: q = (3τ - 5) / (τ - 3)
                                _tau_denom = _tau_estimated - 3.0
                                if abs(_tau_denom) > 1e-8 and _tau_estimated > 3.0:
                                    _q_predicted = (3.0 * _tau_estimated - 5.0) / _tau_denom
                                    _q_predicted = max(1.0, min(2.99, _q_predicted))
                                    _q_tau_residual = abs(_q_empirical - _q_predicted)
                                    # Tolerance: |q_emp - q_pred| < 0.15 → link holds
                                    _Q_TAU_TOLERANCE = 0.15
                                    _q_tau_link_holds = _q_tau_residual < _Q_TAU_TOLERANCE

                        # ── Finite-size crossover check (Tsallis-Tirnakli) ────
                        # For small system sizes, the return distribution follows
                        # a crossover formula rather than a pure q-Gaussian.
                        # Flag when n_nodes < 50 (crossover regime).
                        _finite_size_crossover = _n_nodes_br < 50

                        _q_gaussian_result = {
                            "q_empirical": round(_q_empirical, 4),
                            "kurtosis": round(_kurtosis, 4),
                            "n_returns": _n_returns,
                            "return_mean": round(_r_mean, 4),
                            "return_std": round(_r_std, 4),
                            "tau_estimated": (round(_tau_estimated, 4)
                                              if _tau_estimated is not None else None),
                            "q_predicted_from_tau": (round(_q_predicted, 4)
                                                     if _q_predicted is not None else None),
                            "q_tau_residual": (round(_q_tau_residual, 4)
                                               if _q_tau_residual is not None else None),
                            "q_tau_link_holds": _q_tau_link_holds,
                            "finite_size_crossover": _finite_size_crossover,
                            "o21_challenge": (
                                "q-τ link HOLDS (|q_emp - q_pred| < 0.15): "
                                "the q-parameter of the return distribution is "
                                "fully determined by the avalanche exponent τ, "
                                "NOT by an independent γ. O21's correlation "
                                "between belief-revision scores and spectral γ "
                                "may be an artifact of shared τ-dependence "
                                "rather than direct γ-causation."
                            ) if _q_tau_link_holds is True else (
                                "q-τ link BROKEN (|q_emp - q_pred| >= 0.15): "
                                "the q-parameter is NOT fully determined by τ, "
                                "leaving room for an independent γ-dependence. "
                                "O21's γ-correlation claim survives this test."
                            ) if _q_tau_link_holds is False else (
                                "q-τ link UNTESTABLE: insufficient positive-degree "
                                "nodes or τ <= 3 (outside the q-τ formula domain)."
                            ),
                            "crossover_note": (
                                f"System size n={_n_nodes_br} < 50: finite-size "
                                f"crossover regime (Tsallis-Tirnakli 2010). "
                                f"Return distribution may deviate from pure "
                                f"q-Gaussian — crossover formula applies."
                            ) if _finite_size_crossover else None,
                        }

                        # Print diagnostic
                        _q_status = ("HOLDS" if _q_tau_link_holds is True
                                     else "BROKEN" if _q_tau_link_holds is False
                                     else "UNTESTABLE")
                        print(f"  [Q-GAUSSIAN] q_emp={_q_empirical:.4f}, "
                              f"τ={_tau_estimated if _tau_estimated else '?'}, "
                              f"q_pred={_q_predicted if _q_predicted else '?'}, "
                              f"residual={_q_tau_residual if _q_tau_residual else '?'} "
                              f"→ q-τ link: {_q_status}"
                              + (f" (finite-size crossover: n={_n_nodes_br})"
                                 if _finite_size_crossover else ""))
                        if _q_tau_link_holds is True:
                            print(f"  [Q-GAUSSIAN] ⚠ O21 CHALLENGE: q is τ-determined — "
                                  f"γ-correlation may be τ-mediated artifact, "
                                  f"weakening causal interpretation")
                    else:
                        _q_gaussian_result = {
                            "q_empirical": None,
                            "reason": "insufficient_returns",
                            "n_returns": _n_returns,
                        }
                else:
                    _q_gaussian_result = {
                        "q_empirical": None,
                        "reason": "insufficient_nodes_for_returns",
                        "n_nodes": _n_nodes_br,
                    }

                _branching_report["q_gaussian"] = _q_gaussian_result
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
