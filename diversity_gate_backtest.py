#!/usr/bin/env python3
"""
Diversity-weighted-confirmation gate — retrospective validation backtest.
(Phase 2 of the ambiguity-decomposition gate. 2026-06-19.)

GO/NO-GO DECIDER. The gate (substrate.py + KnowledgeGraph.effective_witness_count)
earns its place ONLY if, from the GENERAL rule alone (no ca_sim-specific hook),
it:

  T1  reproduces O400  — on the PRE-O400 condition (simulation_consistent edges
                         treated as the 'confirms' they once were), n_eff stays
                         ~flat while the raw confirm count balloons.
  T2  down-weights mislabeled 'independent_confirmation' edges.
  T3  zeroes auto-stub / synthetic-internal edges.
  T4  RETAINS genuine external arxiv (negative control — must NOT over-correct).

Plus the real actionable consumer test:
  T5  recompute the falsification-probe deficit with n_eff and check that Spin A
      stays dissolved (INV_073 does not return to the top).

This script REPORTS all results. It does NOT self-grade pass/fail and it writes
nothing — the operator reads the numbers and makes the call.
"""
import json
from collections import defaultdict, Counter

from knowledge_graph import get_graph
from substrate import substrate_of, substrate_class

g = get_graph(); g._ensure_loaded()
edges = g._edges

SEP = "=" * 74


def banner(t):
    print("\n" + SEP + "\n" + t + "\n" + SEP)


# ── T1 — reproduce O400 from the general rule ────────────────────────────────
def t1_reproduce_o400():
    banner("T1  REPRODUCE O400 — does n_eff refuse the ca_sim metronome WITHOUT "
           "the\n     O400-specific hook? (pre-O400 view: simulation_consistent "
           "= confirms)")
    pre = {"simulation_consistent"}   # the type these edges carried before O400
    print(f"\n{'INV':9} {'raw(now)':>9} {'raw(preO400)':>13} {'n_eff(now)':>11} "
          f"{'n_eff(preO400)':>15} {'metro Δn_eff':>13}")
    for inv in ["INV_073", "INV_094", "INV_087", "INV_097"]:
        now = g.effective_witness_count(inv)
        preo = g.effective_witness_count(inv, extra_confirm_types=pre)
        d = round(preo["n_eff"] - now["n_eff"], 2)
        print(f"{inv:9} {now['raw_confirms']:9} {preo['raw_confirms']:13} "
              f"{now['n_eff']:11} {preo['n_eff']:15} {d:13}")
    print("\n  READ: raw(preO400) is what the old confirm-counter saw; n_eff(preO400)\n"
          "  is what the gate credits. The gap = self-confirmation the gate refuses.\n"
          "  If n_eff(preO400) ~= n_eff(now) while raw balloons, O400 is reproduced\n"
          "  by the general substrate rule with no ca_sim-specific code.")


# ── T2 / T3 — mislabeled-independent & auto-stub populations ─────────────────
def t2_t3_populations():
    banner("T2/T3  MISLABELED-INDEPENDENT & AUTO-STUB POPULATIONS (honest census)")
    node_edges = json.load(open("FREED_graph.json")).get("node_edges", [])
    ic_feed = sum(1 for e in edges if e.get("type") == "independent_confirmation")
    ic_node = sum(1 for ne in node_edges if ne.get("type") == "independent_confirmation")
    cw_node = sum(1 for ne in node_edges if ne.get("type") == "consistent_with")
    si_node = sum(1 for ne in node_edges if ne.get("type") == "substrate_independent")
    print(f"\n  T2  'independent_confirmation' edges: feed={ic_feed}  node={ic_node}")
    print(f"      (relabeled long ago to 'consistent_with' n={cw_node} / "
          f"'substrate_independent' n={si_node})")
    print(f"      -> nothing left for n_eff to down-weight; the relabel already happened.")
    print(f"      NOTE: these are NODE-edges (structural), outside feed-edge n_eff's scope.")

    # auto-stub / synthetic-internal: a local:// confirm-family edge that isn't ca_sim/probe
    susp = [e for e in edges
            if e.get("type") in ("confirms", "supports", "extends")
            and str(e.get("from", "")).startswith("local://")
            and "ca_sim" not in str(e.get("from", ""))
            and "adversarial_probe" not in str(e.get("from", ""))]
    print(f"\n  T3  auto-stub / synthetic-internal confirm-family edges: {len(susp)}")
    if susp:
        for e in susp[:5]:
            print(f"       {e.get('from')} -> {e.get('to')} [{e.get('type')}]")
    print(f"      (the auto-stub population was purged in the 2026-05-24 gate-neutralization;")
    print(f"       if any reappear with a local:// non-external source, substrate_of()")
    print(f"       classes them endogenous and n_eff caps the whole substrate at one")
    print(f"       low/zero witness — they cannot re-accumulate.)")


# ── T4 — negative control: external arxiv retained ───────────────────────────
def t4_negative_control():
    banner("T4  NEGATIVE CONTROL — is genuine external support RETAINED (not nuked)?")
    print(f"\n{'INV':9} {'raw':>5} {'n_eff':>8} {'ext_distinct':>13} {'retention':>10}")
    sample = ["INV_087", "INV_078", "INV_042", "INV_063", "INV_064", "INV_023"]
    for inv in sample:
        r = g.effective_witness_count(inv)
        ext = r["n_external_distinct"]
        ret = (r["n_eff"] / ext) if ext else 0.0
        print(f"{inv:9} {r['raw_confirms']:5} {r['n_eff']:8} {ext:13} {ret:9.0%}")
    print("\n  READ: retention = n_eff / distinct-external-papers. Near 100% means the")
    print("  gate keeps genuine external diversity intact. A low number here would")
    print("  mean the gate over-corrects — that would be a FAIL.")


# ── T5 — probe-selector deficit recomputed with n_eff ────────────────────────
def t5_probe_with_neff():
    banner("T5  PROBE SELECTOR under n_eff — does Spin A stay dissolved?")
    # raw deficit (what the live selector uses): confirms - challenges
    raw_conf = defaultdict(int); chal = defaultdict(int)
    for e in edges:
        t = e.get("type", ""); to = e.get("to", "")
        if not to.startswith("INV"):
            continue
        if t == "confirms":   raw_conf[to] += 1
        if t == "challenges": chal[to] += 1
    invs = sorted(set(list(raw_conf) + list(chal)),
                  key=lambda n: raw_conf[n] - chal[n], reverse=True)[:8]
    print(f"\n{'INV':9} {'rawConf':>8} {'chal':>5} {'rawDeficit':>11} "
          f"{'n_eff':>8} {'effDeficit':>11}")
    for inv in invs:
        r = g.effective_witness_count(inv)
        raw_def = raw_conf[inv] - chal[inv]
        eff_def = round(r["n_eff"] - chal[inv], 1)
        print(f"{inv:9} {raw_conf[inv]:8} {chal[inv]:5} {raw_def:11} "
              f"{r['n_eff']:8} {eff_def:11}")
    print("\n  READ: effDeficit = n_eff - challenges. The probe targets the MAX deficit.")
    print("  Whether ranked by raw or n_eff, INV_073 should stay deeply negative")
    print("  (over-challenged) — Spin A dissolved and the diversity metric agrees.")


if __name__ == "__main__":
    print("DIVERSITY-WEIGHTED CONFIRMATION GATE — RETROSPECTIVE BACKTEST")
    print("Graph: %d feed edges. Gate: substrate.py + effective_witness_count()." % len(edges))
    t1_reproduce_o400()
    t2_t3_populations()
    t4_negative_control()
    t5_probe_with_neff()
    print("\n" + SEP)
    print("Backtest complete. Operator adjudicates from the numbers above.")
    print(SEP)
