"""
substrate.py — Provenance/substrate classification for diversity-weighted confirmation.

PHASE 0 of the ambiguity-decomposition gate (Krogh & Vedelsby 1995):
    ensemble_error = mean_member_error - mean_diversity
A confirmation's value is its DECORRELATION from the claim's own generating
substrate. A confirmation produced by the same process that produced the claim
carries ~zero independent information -> it must not move the confirmation total.

This module answers two questions and nothing else:
  1. substrate_of(edge)        -> what process produced this edge?  (regex on `from`)
  2. is_instantiating(sub, to) -> is that substrate correlated-by-construction
                                   with the target claim? (operator-owned table)

It GENERATES NOTHING and WRITES NOTHING. Pure read-and-classify.

NOTE: provenance is derived entirely from the edge `from` field. As of
2026-06-19 that covers ~100% of edges (no edge carries a `source` field).
Adding a new evidence source means adding one row to _SUBSTRATE_PATTERNS.
"""

import re

# ─────────────────────────────────────────────────────────────────────────────
# Substrate taxonomy. Order matters — first match wins. Matched against `from`.
# ─────────────────────────────────────────────────────────────────────────────
_SUBSTRATE_PATTERNS = [
    # ── ENDOGENOUS: produced by FREED itself, correlated-by-construction ──────
    ("ca_sim",          "local://ca_sim"),            # Game-of-Truth CA (simulation_observer.py)
    ("probe_synthetic", "local://adversarial_probe"), # FREED's own falsification probe
    # ── EXTERNAL: produced by the world, decorrelated from FREED's derivation ─
    ("arxiv",           "arxiv.org"),
    ("semanticscholar", "semanticscholar.org"),
    ("biorxiv",         "biorxiv"),
]

# External journal/aggregator domains — all map to 'external_journal'.
# (Distinct label kept so the n_eff formula can later detect same-paper overlap
#  between arxiv/ss/journal indexings of one work; for class purposes all external.)
_EXTERNAL_JOURNAL_DOMAINS = (
    "doi.org", "mdpi.com", "aip.org", "aps.org", "nature.com", "springer.com",
    "frontiersin.org", "sciencemag.org", "hindawi.com", "ethz.ch",
    "epj-conferences.org", "e3s-conferences.org", "research-collection",
)

# Which substrates are FREED-internal (their errors are correlated with FREED's
# own claims by construction). Everything else is treated as external/independent.
_ENDOGENOUS = {"ca_sim", "probe_synthetic", "local_other"}

# WEIGHT TIERS (consumed by Phase 1 effective_witness_count). Documented here so
# the substrate decision and the weighting live in one place:
#   tier 0  w = 0.0   instantiating: endogenous AND is_instantiating(sub,target)
#                     -> the instrument measuring its own setpoint (ca_sim->INV_073/094/087)
#   tier 1  w = low   endogenous, non-instantiating (ca_sim->INV_097): same author,
#                     but observing something the CA was NOT built to produce -> weak
#                     independent signal, kept but never full weight.
#   tier 2  w = 1.0   external (arxiv/ss/journal/biorxiv/manual): decorrelated from
#                     FREED's derivation; subject to same-paper dedup inside n_eff.
ENDOGENOUS_NONINSTANTIATING_WEIGHT = 0.2  # tier-1 weight; tune in Phase 2 backtest


def substrate_of(edge):
    # type: (dict) -> str
    """Return the fine-grained substrate label for an edge, from its `from` field."""
    src = str(edge.get("from", "")).lower()
    for label, pat in _SUBSTRATE_PATTERNS:
        if pat in src:
            return label
    if any(dom in src for dom in _EXTERNAL_JOURNAL_DOMAINS):
        return "external_journal"
    if src.startswith("local://"):
        return "local_other"          # l7-derived / genome-internal / other FREED output
    if src.startswith("http"):
        return "web_external"
    # No URL. Fall back to title: FREED-internal markers stay endogenous, anything
    # else with a real title is a human/manually-submitted external source.
    title = str(edge.get("from_title", "")).lower()
    if "ca telemetry" in title or "game of truth" in title or "adversarial probe" in title:
        return "ca_sim"
    if title.strip():
        return "external_manual"      # human-submitted (e.g. Facebook post, hand link)
    return "unknown"


def substrate_class(substrate):
    # type: (str) -> str
    """Collapse a fine substrate label to 'endogenous' | 'external' | 'unknown'."""
    if substrate in _ENDOGENOUS:
        return "endogenous"
    if substrate == "unknown":
        return "unknown"
    return "external"


# ─────────────────────────────────────────────────────────────────────────────
# OPERATOR-OWNED TABLE — DRAFT, awaiting David's sign-off.
#
#   This is the ONE place subjective judgment enters the gate, and it is the one
#   thing that must NEVER be daemon-derived (a daemon that writes its own
#   provenance map and reads it back is the ca_sim bug one level up). Edit by
#   hand. Version-controlled. The daemon only reads it.
#
#   A claim is "CA-instantiated" when the Game-of-Truth CA was BUILT TO EMBODY
#   the observable the claim asserts. For such claims a ca_sim confirmation is
#   the instrument measuring what it was tuned to produce -> weight 0.
#   For claims the CA does NOT embody, a ca_sim observation is at least partly
#   independent and is NOT auto-zeroed here.
# ─────────────────────────────────────────────────────────────────────────────
CA_INSTANTIATED = {
    # claim   : (zero_ca_sim, confidence, rationale)
    "INV_073": (True,  "high",
                "Semantic Transport Field / critical-ridge navigation. CA produces "
                "sigma~1.0 + power-law avalanching BY CONSTRUCTION (AT_CRITICAL). "
                "This is the O400 case, already confirmed."),
    "INV_094": (True,  "high",
                "Neural Signature of Criticality (gamma=1 temporal). CA telemetry "
                "literally measures sigma/alpha/H and is tuned to sit at criticality. "
                "ca_sim confirmation = instrument measuring its own tuning."),
    "INV_087": (True,  "high (adjudicated 2026-06-19, CC-predicted, DHF-delegated)",
                "MaxRL as Thermodynamic Necessity. The CA is TUNED to gamma=1, which "
                "IS MaxRL's signature -> a ca_sim confirmation here is the instrument "
                "measuring its own setpoint. Correlated by construction -> w=0. NOTE: "
                "the CA can earn weight back on this claim by being rebuilt to run a "
                "CONTROLLED MaxRL-vs-standard-RL experiment; that upgraded sim is a NEW "
                "substrate (not in this zero-set), and would witness the *mechanism* "
                "rather than echo the *setpoint*. Keep building it; don't abandon."),
    "INV_097": (False, "high (adjudicated 2026-06-19, CC-predicted, DHF-delegated)",
                "Topological Prepayment (conservation laws = one-time thermodynamic "
                "payment; stable particles biject with conservation laws). The CA was "
                "NOT built to embody this (its hidden physics is Mandelbrot zoom). So a "
                "ca_sim hit is the rare case of the sim surprising its author -> NOT "
                "auto-zeroed. But it is still endogenous/same-author, so n_eff gives it "
                "the LOW 'endogenous-non-instantiating' tier, never full external "
                "weight. 1 edge only."),
    "INV_070": (False, "LOW — please adjudicate",
                "Semantic death by cold. Plausibly mapped to the CA's Symbol-Drifter "
                "divergence tax / death, which WOULD make it CA-instantiated. But its "
                "2 ca_sim edges are CHALLENGES, not confirms — no confirm-inflation "
                "risk either way. Left out of the zero-set for now."),
}


def is_instantiating(substrate, target):
    # type: (str, str) -> bool
    """
    True if `substrate` is correlated-by-construction with claim `target`
    (=> a confirmation from it carries ~zero independent information => w=0).
    Currently only the ca_sim/CA-instantiated relation is encoded.
    """
    if substrate == "ca_sim":
        entry = CA_INSTANTIATED.get(target)
        return bool(entry and entry[0])
    return False


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    from collections import Counter
    g = json.load(open("FREED_graph.json"))
    edges = g["edges"]
    print("substrate distribution over ALL %d edges:" % len(edges))
    for k, v in Counter(substrate_of(e) for e in edges).most_common():
        print("  %5d  %-18s [%s]" % (v, k, substrate_class(k)))
    print("\nunknown-substrate edges (should be ~0):")
    for e in edges:
        if substrate_of(e) == "unknown":
            print("  ", str(e.get("from", ""))[:70], "->", e.get("to"))
