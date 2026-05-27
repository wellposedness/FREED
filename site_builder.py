"""
FREED — Site Builder
Generates a static website into docs/ after every cycle.
GitHub Pages serves docs/ — the world sees FREED working in real time.

The site is the daemon explaining itself.
It does not describe FREED. It IS FREED's output surface.
"""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

FREED_DIR  = Path(__file__).parent
DOCS_DIR   = FREED_DIR / "docs"
DOCS_DIR.mkdir(exist_ok=True)

CYCLES_LOG = DOCS_DIR / "cycles.json"
MAX_CYCLES = 50   # keep last 50 cycles in public log


def build(state: dict, obligations: list, cycle_log: dict = None):
    """
    Called by freed.py after every UPDATE phase.
    Writes state.json, obligations.json, cycles.json, index.html, then pushes to GitHub.
    """
    _write_state(state)
    _write_obligations(obligations)
    _write_cycles(cycle_log)
    _write_symbols()
    _write_wiring()
    _write_index()
    print("[SITE] docs/ updated.")
    _push(state.get("generation", "?"))


# ── Git push ─────────────────────────────────────────────────────────────────

def _push(generation):
    """Stage changed files and push to GitHub. Silent on nothing-to-push."""
    try:
        subprocess.run(
            ["git", "add", "docs/", "FREED_state.json", "FREED_obligations.json"],
            cwd=FREED_DIR, check=True, capture_output=True,
        )
        result = subprocess.run(
            ["git", "commit", "-m", f"Gen {generation} — FREED cycle"],
            cwd=FREED_DIR, capture_output=True, text=True,
        )
        if "nothing to commit" in result.stdout:
            print("[SITE] Nothing new to push.")
            return
        subprocess.run(
            ["git", "push"],
            cwd=FREED_DIR, check=True, capture_output=True,
        )
        print(f"[SITE] Pushed gen {generation} to GitHub Pages.")
    except subprocess.CalledProcessError as e:
        print(f"[SITE] Push failed: {e.stderr or e}")


# ── Data files ────────────────────────────────────────────────────────────────

def _write_state(state: dict):
    out = dict(state)
    out["generated"] = datetime.now(timezone.utc).isoformat()
    (DOCS_DIR / "state.json").write_text(json.dumps(out, indent=2))


def _write_obligations(obligations: list):
    (DOCS_DIR / "obligations.json").write_text(
        json.dumps(obligations, indent=2, ensure_ascii=False)
    )


def _write_symbols():
    """Copy genome_symbols.json to docs/ for browser access."""
    src = FREED_DIR / "genome_symbols.json"
    if src.exists():
        (DOCS_DIR / "symbols.json").write_text(src.read_text(encoding="utf-8"))


def _write_wiring():
    """Publish WIRING.md to docs/ so it's accessible at the site root."""
    src = FREED_DIR / "WIRING.md"
    if src.exists():
        (DOCS_DIR / "wiring.md").write_text(src.read_text(encoding="utf-8"))


def _write_noethers_table():
    """Guard for the hand-edited Noether's Table page.
    ENGINEER: DO NOT MODIFY this function or docs/noethers_table.html. The file is hand-curated.
    The sentinel check below is intentional — it must not be removed.
    """
    target = DOCS_DIR / "noethers_table.html"
    if target.exists() and "HAND-EDITED" in target.read_text(encoding="utf-8"):
        return


def _write_game_of_life():
    """CA pages have moved to wellposedness/FREED-CA (separate repo).
    ENGINEER: DO NOT MODIFY or recreate this function. The CA is intentionally
    absent from this repo — it lives at wellposedness.github.io/FREED-CA/
    """
    pass  # CA moved to FREED-CA repo — do not write game_of_life.html here


def _extract_criticality_verdict(ca_telemetry: dict) -> dict:
    """Extract branching-ratio σ, power-law exponent α, and derive a
    machine-readable criticality_verdict from a CA telemetry snapshot.

    Returns a dict with keys: branching_ratio, branching_ratio_err,
    shannon_entropy, avalanche_exponent, power_law_r2, survival_rate,
    criticality_verdict.  Empty dict when no telemetry is available.
    """
    if not ca_telemetry:
        return {}

    sigma = ca_telemetry.get("branching_ratio")
    sigma_err = ca_telemetry.get("branching_ratio_err", 0.0)
    alpha = ca_telemetry.get("power_law_exponent") or ca_telemetry.get("avalanche_exponent")
    r2 = ca_telemetry.get("power_law_r2")
    entropy = ca_telemetry.get("shannon_entropy")
    survival = ca_telemetry.get("survival_rate")

    # Derive verdict from JOINT evidence: σ band AND α power-law exponent.
    # σ critical band: [1.0 − 0.05, 1.0 + 0.05]
    # α expected range for SOC: [2.0, 3.0] with power-law R² > 0.7
    # Verdict is falsifiable per-generation: both must agree for AT_CRITICAL.
    sigma_verdict = None
    if sigma is not None:
        try:
            s = float(sigma)
            if abs(s - 1.0) <= 0.05:
                sigma_verdict = "AT_CRITICAL"
            elif s > 1.05:
                sigma_verdict = "SUPERCRITICAL"
            else:
                sigma_verdict = "SUBCRITICAL"
        except (TypeError, ValueError):
            sigma_verdict = "UNKNOWN"

    alpha_verdict = None
    if alpha is not None and r2 is not None:
        try:
            a = float(alpha)
            r2_val = float(r2)
            if 2.0 <= a <= 3.0 and r2_val > 0.7:
                alpha_verdict = "POWER_LAW_CONFIRMED"
            elif r2_val <= 0.7:
                alpha_verdict = "POWER_LAW_WEAK"
            else:
                alpha_verdict = "POWER_LAW_OUT_OF_BAND"
        except (TypeError, ValueError):
            alpha_verdict = "UNKNOWN"

    # Joint verdict: AT_CRITICAL requires BOTH σ in band AND α confirmed
    verdict = None
    verdict_basis = []
    if sigma_verdict is not None:
        verdict_basis.append("sigma=" + sigma_verdict)
    if alpha_verdict is not None:
        verdict_basis.append("alpha=" + alpha_verdict)

    if sigma_verdict == "AT_CRITICAL" and alpha_verdict == "POWER_LAW_CONFIRMED":
        verdict = "AT_CRITICAL"
    elif sigma_verdict == "AT_CRITICAL" and alpha_verdict is None:
        verdict = "AT_CRITICAL_SIGMA_ONLY"
    elif sigma_verdict == "SUPERCRITICAL":
        verdict = "SUPERCRITICAL"
    elif sigma_verdict == "SUBCRITICAL":
        verdict = "SUBCRITICAL"
    elif sigma_verdict == "AT_CRITICAL" and alpha_verdict in ("POWER_LAW_WEAK", "POWER_LAW_OUT_OF_BAND"):
        verdict = "CRITICAL_CONTESTED"
    elif sigma_verdict is not None:
        verdict = sigma_verdict
    # If only α available (no σ), still record
    elif alpha_verdict is not None:
        verdict = "ALPHA_ONLY_" + alpha_verdict

    result = {}
    if sigma is not None:
        result["branching_ratio"] = sigma
    if sigma_err:
        result["branching_ratio_err"] = sigma_err
    if entropy is not None:
        result["shannon_entropy"] = entropy
    if alpha is not None:
        result["avalanche_exponent"] = alpha
    if r2 is not None:
        result["power_law_r2"] = r2
    if survival is not None:
        result["survival_rate"] = survival
    if verdict is not None:
        result["criticality_verdict"] = verdict
    if verdict_basis:
        result["verdict_basis"] = verdict_basis

    # O148: Track frozen-seed count — permanently-active cells that act as
    # catalytic substrates for phase-stratified emergence (per frozen-GoL paper).
    # Without this variable the measurement protocol cannot detect the causally
    # decisive phase transitions that survival_rate alone misses.
    frozen_seeds = ca_telemetry.get("frozen_seed_count")
    if frozen_seeds is not None:
        result["frozen_seed_count"] = frozen_seeds

    # O148: Capture dominant_cell_type — the cell type with the highest count
    # in the CA grid. This enables tracking whether the dominant functional
    # class shifts across generations, which would indicate a phase transition
    # in the CA's self-organized structure.
    dominant_type_val = ca_telemetry.get("dominant_type") or ca_telemetry.get("dominant_cell_type")
    dominant_count = ca_telemetry.get("dominant_count") or ca_telemetry.get("dominant_cell_count")
    if dominant_type_val is not None:
        result["dominant_cell_type"] = dominant_type_val
    if dominant_count is not None:
        result["dominant_cell_count"] = dominant_count

    # INV_023 / O148: Closure-integrity check — distinguish autopoietic
    # circular-closure collapse from simple cell death.  Autopoietic closure
    # requires that boundary-generating processes are themselves produced by
    # internal cell state (R[R]=R at the substrate level).  When the CA
    # telemetry supplies boundary_producers (count of cells whose boundary
    # outputs are regenerated by internal processes) and total_boundary_cells,
    # we compute a closure_ratio.  A cell that dies with closure_ratio < 1.0
    # died from closure collapse (its boundary generators were not self-
    # produced), which is categorically different from survival failure where
    # closure was maintained but resources were insufficient.
    #
    # This makes the CA simulation a direct empirical test of the circular-
    # organisation hypothesis rather than a proxy survival metric, and
    # challenges INV_023's implicit allowance of functional simulation as
    # equivalent to genuine substrate-level self-production.
    boundary_producers = ca_telemetry.get("boundary_producers")
    total_boundary = ca_telemetry.get("total_boundary_cells")
    cell_deaths = ca_telemetry.get("cell_deaths", 0)
    closure_deaths_raw = ca_telemetry.get("closure_deaths")

    closure_ratio = None
    if boundary_producers is not None and total_boundary is not None:
        try:
            bp = float(boundary_producers)
            tb = float(total_boundary)
            if tb > 0:
                closure_ratio = round(bp / tb, 6)
                result["closure_ratio"] = closure_ratio
                result["boundary_producers"] = boundary_producers
                result["total_boundary_cells"] = total_boundary
                # Closure intact when every boundary cell's generator is
                # itself produced by internal state (ratio >= 1.0)
                result["closure_intact"] = closure_ratio >= 1.0
            elif tb == 0 and bp == 0:
                # No boundary at all — trivially closed (empty system)
                result["closure_ratio"] = 1.0
                result["closure_intact"] = True
        except (TypeError, ValueError):
            pass

    # Decompose deaths into closure-collapse vs survival-failure when data
    # is available.  closure_deaths = cells that died while closure_ratio < 1
    # (their boundary generators were not self-produced).  survival_deaths =
    # cells that died despite maintaining closure (resource/competition loss).
    if closure_deaths_raw is not None:
        try:
            cd = int(closure_deaths_raw)
            sd = int(cell_deaths) - cd if cell_deaths else 0
            result["closure_deaths"] = cd
            result["survival_deaths"] = max(sd, 0)
            result["death_mode"] = (
                "CLOSURE_COLLAPSE" if cd > max(sd, 0)
                else "SURVIVAL_FAILURE" if sd > cd
                else "MIXED" if cd > 0 and sd > 0
                else "NONE"
            )
        except (TypeError, ValueError):
            pass
    elif closure_ratio is not None and cell_deaths:
        # Infer death mode from closure_ratio when per-cell death
        # categorisation is not available from the CA runner
        try:
            cd_count = int(cell_deaths)
            if closure_ratio < 1.0 and cd_count > 0:
                # Estimate: fraction of deaths attributable to closure loss
                # proportional to the closure deficit
                closure_deficit = 1.0 - closure_ratio
                estimated_closure_deaths = int(round(cd_count * min(closure_deficit, 1.0)))
                result["closure_deaths_estimated"] = estimated_closure_deaths
                result["survival_deaths_estimated"] = cd_count - estimated_closure_deaths
                result["death_mode"] = (
                    "CLOSURE_COLLAPSE_ESTIMATED" if estimated_closure_deaths > cd_count // 2
                    else "SURVIVAL_FAILURE_ESTIMATED"
                )
            elif cd_count > 0:
                result["death_mode"] = "SURVIVAL_FAILURE"
                result["survival_deaths_estimated"] = cd_count
        except (TypeError, ValueError):
            pass

    # Closure-integrity verdict for time-series tracking
    if closure_ratio is not None:
        if closure_ratio >= 1.0:
            result["closure_verdict"] = "R[R]=R_MAINTAINED"
        elif closure_ratio >= 0.8:
            result["closure_verdict"] = "CLOSURE_DEGRADED"
        elif closure_ratio >= 0.5:
            result["closure_verdict"] = "CLOSURE_FAILING"
        else:
            result["closure_verdict"] = "CLOSURE_COLLAPSED"

    # Per-cell-type branching decomposition: determine whether Navigator cells
    # are causally responsible for maintaining σ≈1.02 or merely coincidentally
    # dominant.  When the CA runner supplies per_type_branching or cell_type_counts,
    # we compute each type's contribution to aggregate σ.  Otherwise we derive
    # what we can from dominant_type + total σ.
    per_type_raw = ca_telemetry.get("per_type_branching")
    cell_type_counts = ca_telemetry.get("cell_type_counts")
    dominant_type = ca_telemetry.get("dominant_type")

    per_type_branching = {}
    if isinstance(per_type_raw, dict) and per_type_raw:
        # Full per-type data from CA runner: {type_name: {sigma, count, ...}}
        total_cells = sum(
            (v.get("count", 0) if isinstance(v, dict) else 0)
            for v in per_type_raw.values()
        )
        for ctype, cdata in per_type_raw.items():
            if not isinstance(cdata, dict):
                continue
            ct_sigma = cdata.get("sigma")
            ct_count = cdata.get("count", 0)
            entry = {}
            if ct_sigma is not None:
                entry["sigma"] = ct_sigma
            if ct_count:
                entry["count"] = ct_count
            if total_cells and ct_count and sigma is not None:
                # Weighted contribution to aggregate σ
                weight = ct_count / total_cells
                entry["weight"] = round(weight, 6)
                try:
                    entry["sigma_contribution"] = round(float(ct_sigma) * weight, 6)
                except (TypeError, ValueError):
                    pass
            if entry:
                per_type_branching[ctype] = entry
    elif isinstance(cell_type_counts, dict) and cell_type_counts and sigma is not None:
        # Only counts available — attribute aggregate σ proportionally (null
        # hypothesis: all types branch at the same rate).  This is the baseline
        # against which per-type σ data, when available, can be compared.
        total_cells = sum(cell_type_counts.values())
        for ctype, ct_count in cell_type_counts.items():
            if total_cells:
                weight = ct_count / total_cells
                per_type_branching[ctype] = {
                    "count": ct_count,
                    "weight": round(weight, 6),
                    "sigma_contribution_null_hypothesis": round(float(sigma) * weight, 6),
                }
    elif dominant_type and sigma is not None:
        # Minimal info: record that dominant type exists alongside aggregate σ
        per_type_branching[dominant_type] = {
            "dominant": True,
            "aggregate_sigma": sigma,
            "note": "per-type sigma unavailable; only aggregate recorded",
        }

    if per_type_branching:
        # Per-type criticality verdicts: classify each cell type individually
        for ctype, cdata in per_type_branching.items():
            ct_sigma = cdata.get("sigma")
            if ct_sigma is not None:
                try:
                    s_val = float(ct_sigma)
                    if abs(s_val - 1.0) <= 0.05:
                        cdata["verdict"] = "AT_CRITICAL"
                    elif s_val > 1.05:
                        cdata["verdict"] = "SUPERCRITICAL"
                    else:
                        cdata["verdict"] = "SUBCRITICAL"
                except (TypeError, ValueError):
                    cdata["verdict"] = "UNKNOWN"

        result["per_type_branching"] = per_type_branching

        # Heterogeneity diagnostic: are per-type σ values genuinely distributed
        # or is criticality load-bearing on a single functional class?
        typed_sigmas = {}
        for ctype, cdata in per_type_branching.items():
            ct_sigma = cdata.get("sigma")
            if ct_sigma is not None:
                try:
                    typed_sigmas[ctype] = float(ct_sigma)
                except (TypeError, ValueError):
                    pass

        if len(typed_sigmas) >= 2:
            sigma_vals = list(typed_sigmas.values())
            mean_ts = sum(sigma_vals) / len(sigma_vals)
            std_ts = (sum((v - mean_ts) ** 2 for v in sigma_vals) / len(sigma_vals)) ** 0.5
            result["per_type_sigma_mean"] = round(mean_ts, 6)
            result["per_type_sigma_std"] = round(std_ts, 6)
            # If std is low relative to band width, criticality is distributed;
            # if high, it's load-bearing on specific types
            result["criticality_distributed"] = std_ts <= 0.025
            # Identify load-bearing types (those individually AT_CRITICAL with
            # weight > 0.3 of total cells)
            load_bearing = [
                ctype for ctype, cdata in per_type_branching.items()
                if cdata.get("verdict") == "AT_CRITICAL"
                and cdata.get("weight", 0) > 0.3
            ]
            if load_bearing:
                result["load_bearing_critical_types"] = load_bearing

        # Flag whether Navigator cells drive criticality
        nav_entries = {k: v for k, v in per_type_branching.items()
                       if "navigator" in k.lower()}
        if nav_entries:
            nav_total_contrib = sum(
                v.get("sigma_contribution", v.get("sigma_contribution_null_hypothesis", 0.0))
                for v in nav_entries.values()
            )
            result["navigator_branching_contribution"] = round(nav_total_contrib, 6)
            # Per-type verdict for Navigator class specifically
            nav_sigmas = [v.get("sigma") for v in nav_entries.values()
                          if v.get("sigma") is not None]
            if nav_sigmas:
                nav_mean = sum(float(s) for s in nav_sigmas) / len(nav_sigmas)
                result["navigator_sigma_mean"] = round(nav_mean, 6)
                if abs(nav_mean - 1.0) <= 0.05:
                    result["navigator_verdict"] = "AT_CRITICAL"
                elif nav_mean > 1.05:
                    result["navigator_verdict"] = "SUPERCRITICAL"
                else:
                    result["navigator_verdict"] = "SUBCRITICAL"

    return result


def _write_cycles(cycle_log: dict):
    """Append the latest cycle to the rolling log."""
    if not cycle_log:
        return

    existing = []
    if CYCLES_LOG.exists():
        existing = json.loads(CYCLES_LOG.read_text())

    # Extract CA criticality metrics when telemetry is present
    ca_telemetry = cycle_log.get("phases", {}).get("ca_telemetry", {})
    criticality = _extract_criticality_verdict(ca_telemetry)

    # Extract a clean public summary from the cycle log
    summary = {
        "cycle":      cycle_log.get("cycle"),
        "generation": cycle_log.get("generation"),
        "timestamp":  cycle_log.get("timestamp"),
        "sweep":      cycle_log.get("phases", {}).get("sweep", {}),
        "feed":       [
            {"title": f.get("title","?"), "compress": f.get("compress","")}
            for f in cycle_log.get("phases", {}).get("feed", [])
        ],
        "resolve":    cycle_log.get("phases", {}).get("resolve", {}),
        "coherence":  cycle_log.get("phases", {}).get("update", {}).get("coherence"),
    }

    # Embed all four criticality metrics so every snapshot is self-describing
    # (branching_ratio, shannon_entropy, avalanche_exponent, criticality_verdict
    #  plus survival_rate) — closes O148 continuous-verifiability gap.
    if criticality:
        summary["criticality"] = criticality

    # Record σ and α time-series from CA telemetry so temporal drift
    # toward/away from criticality is trackable across cycles (resolves O148).
    # When the CA runner provides per-step arrays, embed them directly;
    # otherwise, accumulate point values into a rolling time-series file.
    _append_criticality_timeseries(criticality, summary.get("timestamp"))

    existing.append(summary)
    existing = existing[-MAX_CYCLES:]   # keep rolling window
    CYCLES_LOG.write_text(json.dumps(existing, indent=2, ensure_ascii=False))


def _append_criticality_timeseries(criticality: dict, timestamp: str = None):
    """Append σ and α point values to a rolling time-series JSON file.

    If the CA telemetry includes per-step arrays (sigma_timeseries,
    alpha_timeseries), those are stored verbatim for that cycle.
    Otherwise the snapshot σ and α are appended as single-point entries.
    This enables detection of drift outside the critical band over time,
    falsifying or confirming the attractor claim (INV_073).
    """
    if not criticality:
        return

    ts_file = DOCS_DIR / "criticality_timeseries.json"
    existing = []
    if ts_file.exists():
        try:
            existing = json.loads(ts_file.read_text())
        except (json.JSONDecodeError, OSError):
            existing = []

    entry = {"timestamp": timestamp or datetime.now(timezone.utc).isoformat()}

    # Snapshot values — always present when criticality dict is non-empty
    sigma = criticality.get("branching_ratio")
    sigma_err = criticality.get("branching_ratio_err")
    alpha = criticality.get("avalanche_exponent")
    r2 = criticality.get("power_law_r2")
    verdict = criticality.get("criticality_verdict")

    if sigma is not None:
        entry["sigma"] = sigma
    if sigma_err is not None:
        entry["sigma_err"] = sigma_err
    if alpha is not None:
        entry["alpha"] = alpha
    if r2 is not None:
        entry["power_law_r2"] = r2
    if verdict is not None:
        entry["verdict"] = verdict

    # O148: Log Shannon entropy H and survival rate alongside σ in every
    # time-series entry so the entropy-suppression-at-criticality finding
    # becomes a tracked, falsifiable time series rather than a point observation.
    entropy = criticality.get("shannon_entropy")
    survival = criticality.get("survival_rate")
    if entropy is not None:
        entry["shannon_entropy"] = entropy
    if survival is not None:
        entry["survival_rate"] = survival

    # Per-step arrays from extended CA telemetry (when available)
    for ts_key in ("sigma_timeseries", "alpha_timeseries", "entropy_timeseries"):
        ts_val = criticality.get(ts_key)
        if isinstance(ts_val, list) and ts_val:
            entry[ts_key] = ts_val

    # Compute running drift diagnostic: is σ trending away from 1.0?
    recent_sigmas = [e.get("sigma") for e in existing[-19:] if e.get("sigma") is not None]
    if sigma is not None:
        recent_sigmas.append(sigma)
    if len(recent_sigmas) >= 3:
        mean_sigma = sum(recent_sigmas) / len(recent_sigmas)
        sigma_std = (sum((s - mean_sigma) ** 2 for s in recent_sigmas) / len(recent_sigmas)) ** 0.5
        entry["sigma_rolling_mean"] = round(mean_sigma, 6)
        entry["sigma_rolling_std"] = round(sigma_std, 6)
        entry["sigma_in_critical_band"] = abs(mean_sigma - 1.0) <= 0.05

    existing.append(entry)
    # Keep last 200 entries (matches CA measurement window)
    existing = existing[-200:]
    ts_file.write_text(json.dumps(existing, indent=2, ensure_ascii=False))


# ── HTML ──────────────────────────────────────────────────────────────────────

def _render_projects(projects: list) -> str:
    if not projects:
        return ""
    parts = []
    for n in projects:
        tags_html = ""
        for t in (n.get("invariants") or []):
            tags_html += f'<span class="node-tag inv">{t}</span>'
        for t in (n.get("obligations") or []):
            tags_html += f'<span class="node-tag ob">{t}</span>'
        for t in (n.get("tags") or []):
            tags_html += f'<span class="node-tag">{t}</span>'

        council     = ", ".join(n.get("council") or [])
        drift_class = ' drifting' if n.get("drift_flag") else ''
        _ov = n.get("drift_overlap")
        _ov_str = f'{_ov:.2f}' if isinstance(_ov, (int, float)) else '?'
        drift_html  = (
            f'<div class="node-drift-badge">⚠ DRIFT — compress overlap '
            f'{_ov_str} (re-examine)</div>'
            if n.get("drift_flag") else ''
        )
        parts.append(f"""<div class="node{drift_class}">
  <div class="node-header">
    <span class="node-title">{n.get("title","?")}</span>
    <span class="node-gen">Gen {n.get("generation","?")} · {n.get("created","")}</span>
  </div>
  <div class="node-summary">{n.get("summary","")}</div>
  <div class="node-compress">↳ {n.get("compress","")}</div>
  <div class="node-next">NEXT: {n.get("next","")}</div>
  {f'<div class="node-council">Council: {council}</div>' if council else ''}
  {drift_html}
  <div class="node-tags">{tags_html}</div>
</div>""")
    return "\n".join(parts)


def _render_promotion_queue(candidates: list) -> str:
    """Render genome promotion candidates (invariants with recurrence >= 3)."""
    if not candidates:
        return '<div class="loading">No promotion candidates yet — mine phase needs 3+ independent nodes confirming the same invariant.</div>'
    parts = []
    for c in candidates:
        nodes_in = ", ".join(c.get("appears_in", []))
        rec      = c.get("recurrence", 0)
        parts.append(
            f'<div class="promo-candidate">'
            f'<div class="promo-text">{c.get("invariant","")}</div>'
            f'<div class="promo-meta"><span class="promo-count">{rec}×</span> independent · '
            f'{nodes_in}</div>'
            f'</div>'
        )
    return "\n".join(parts)


def _load_projects() -> list:
    idx = DOCS_DIR / "projects.json"
    if idx.exists():
        return json.loads(idx.read_text())
    return []


def _write_index():
    html = _render_html(_load_projects())
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")


def _render_html(projects: list = None) -> str:
    projects = projects or []
    projects_html = _render_projects(projects)

    # Load promotion candidates from state
    promotion_candidates = []
    try:
        sfile = FREED_DIR / "FREED_state.json"
        if sfile.exists():
            sdata = json.loads(sfile.read_text())
            promotion_candidates = sdata.get("promotion_candidates", [])
    except Exception:
        pass
    promo_html = _render_promotion_queue(promotion_candidates)
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FREED — Freed Recursive Engine for Epistemic Dynamics</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;1,300;1,400&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:      #080808;
    --surface: #111111;
    --border:  #2d2d2d;
    --accent:  #dc2626;
    --green:   #22c55e;
    --amber:   #f59e0b;
    --blue:    #60a5fa;
    --red:     #ef4444;
    --text:    #e5e5e5;
    --muted:   #8b8b8b;
    --mono:    'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
    --serif:   'Cormorant Garamond', 'Palatino Linotype', Georgia, serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body {
    height: 100vh;
    overflow: hidden;
    background: var(--bg);
    color: var(--text);
    font-family: var(--serif);
    font-weight: 300;
    font-size: 15px;
    line-height: 1.6;
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* ── HUD shell ── */
  .hud-shell {
    display: flex;
    flex-direction: column;
    height: 100vh;
  }
  .hud-top {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 0.55rem 1.4rem;
    border-bottom: 1px solid #000;
    flex-shrink: 0;
    flex-wrap: wrap;
  }
  .hud-title {
    font-family: var(--serif);
    font-weight: 300;
    font-size: 1.5rem;
    letter-spacing: 0.18em;
    color: var(--accent);
  }
  .hud-sub {
    font-family: var(--serif);
    font-weight: 300;
    font-size: 0.82rem;
    color: var(--muted);
  }
  .hud-top-divider { color: var(--border); }
  .daemon-status {
    margin-left: auto; display: flex; align-items: center; gap: 0.5rem;
    font-family: var(--mono); font-size: 0.62rem; color: var(--muted);
  }
  .daemon-phase {
    font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase;
    padding: 0.15rem 0.4rem; border: 1px solid currentColor;
  }
  .daemon-phase.idle       { color: var(--green); }
  .daemon-phase.perceive   { color: var(--blue); }
  .daemon-phase.represent  { color: var(--accent); }
  .daemon-phase.compress   { color: var(--amber); }
  .daemon-phase.predict    { color: var(--amber); }
  .daemon-phase.compare    { color: var(--accent); }
  .daemon-phase.adjust     { color: var(--muted); }
  .daemon-phase.repeat     { color: var(--green); }
  .daemon-phase.pre-audit  { color: var(--muted); }
  /* Panel subtitle — kernel step whisper */
  .panel-subtitle {
    font-family: var(--mono); font-size: 0.55rem; letter-spacing: 0.13em;
    color: var(--muted); text-align: center; text-transform: uppercase;
    opacity: 0.65; margin-top: 0.15rem; padding-bottom: 0.35rem;
    border-bottom: 1px solid var(--border); flex-shrink: 0;
  }
  /* Kernel chain progress */
  .kernel-chain {
    display: flex; align-items: center; gap: 0.3rem; margin-left: auto;
    font-family: var(--mono); font-size: 0.58rem; letter-spacing: 0.07em;
  }
  .kstep { color: var(--border); transition: color 0.3s; text-transform: uppercase; }
  .kstep.kstep-active { color: var(--accent); font-weight: 600; }
  .karrow { color: var(--border); font-size: 0.5rem; }
  .daemon-detail { color: var(--muted); max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .hud-grid {
    display: grid;
    grid-template-columns: 30fr 40fr 30fr;
    flex: 1;
    overflow: hidden;
    min-height: 0;
  }
  .hud-panel {
    overflow-y: auto;
    padding: 0.9rem 1.1rem;
    border-right: 1px solid #000;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  .hud-panel:last-child { border-right: none; }
  .hud-footer {
    border-top: 1px solid var(--border);
    padding: 0.35rem 1.4rem;
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-shrink: 0;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  .panel-title {
    font-family: var(--mono);
    font-size: 0.78rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--text);
    text-align: center;
    padding-bottom: 0.1rem;
    flex-shrink: 0;
  }

  /* Panel blocks — law and formulations */
  .formulations {
    padding: 0.6rem 0.85rem;
    background: var(--surface);
    border-left: 3px solid var(--border);
    font-family: var(--mono);
    font-size: 0.72rem;
  }
  .formulations .label { color: var(--muted); font-size: 0.62rem; margin-bottom: 0.4rem; letter-spacing: 0.1em; text-transform: uppercase; }
  .form-chain { color: var(--text); margin-bottom: 0.5rem; font-family: var(--serif); font-style: italic; font-size: 0.82rem; font-weight: 300; }
  .form-chain .arrow { color: var(--accent); margin: 0 0.25rem; font-style: normal; }
  .form-row { display: flex; align-items: baseline; gap: 0.6rem; margin-top: 0.28rem; line-height: 1.5; }
  .form-tag { color: var(--muted); font-size: 0.6rem; letter-spacing: 0.07em; text-transform: uppercase; min-width: 6rem; flex-shrink: 0; }
  .form-expr { color: var(--text); font-size: 0.7rem; }

  /* Pulse indicator — top bar compact circle */
  .pulse {
    display: inline-block;
    width: 22px; height: 22px;
    border-radius: 50%;
    border: 2px solid var(--accent);
    background: transparent;
    flex-shrink: 0;
    animation: pulse 2s infinite;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.2; }
  }
  @keyframes flash-update {
    0%   { background: var(--accent); color: #fff; border-radius: 2px; }
    100% { background: transparent;   color: inherit; }
  }
  .flash-val { animation: flash-update 0.7s ease-out; padding: 0 3px; margin: 0 -3px; }
  /* State panel */
  .state-grid { display: flex; flex-direction: column; gap: 0; margin-bottom: 0.5rem; border: 1px solid var(--green); padding: 0 0.6rem; }
  .state-cell {
    display: flex; justify-content: space-between; align-items: baseline;
    padding: 0.32rem 0;
    border-bottom: 1px solid var(--border);
  }
  .state-cell:last-child { border-bottom: none; }
  .state-cell .label { font-family: var(--mono); color: var(--muted); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.08em; }
  .state-cell .value { font-family: var(--mono); font-size: 0.82rem; color: var(--text); }
  .state-cell .value.accent { color: var(--accent); font-weight: 600; }
  .state-cell .value.green  { color: var(--green);  font-weight: 600; }

  /* Section */
  .section { margin-bottom: 2.5rem; }
  .section-title {
    font-family: var(--mono);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem;
    margin-bottom: 1rem;
  }
  /* Collapsible sections */
  details.section { margin-bottom: 2.5rem; }
  /* Unified summary style — shared by right-column sections and center ob-groups */
  details.section > summary,
  details.ob-group > summary.ob-group-title {
    font-family: var(--mono);
    font-size: 0.68rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem;
    margin-bottom: 1rem;
    cursor: pointer;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    user-select: none;
  }
  details.section > summary::-webkit-details-marker,
  details.ob-group > summary.ob-group-title::-webkit-details-marker { display: none; }
  /* Triangle on the left */
  details.section > summary::before,
  details.ob-group > summary.ob-group-title::before {
    content: '▶';
    font-size: 0.55rem;
    opacity: 0.5;
    transition: transform 0.15s;
    flex-shrink: 0;
  }
  details.section[open] > summary::before,
  details.ob-group[open] > summary.ob-group-title::before { transform: rotate(90deg); opacity: 1; }
  details.section > summary::after,
  details.ob-group > summary.ob-group-title::after {
    content: ' (click to open)'; font-size: 0.58rem; color: var(--muted); opacity: 0.7; font-weight: 400; letter-spacing: 0.04em;
  }
  details.section[open] > summary::after,
  details.ob-group[open] > summary.ob-group-title::after { content: ''; }
  details.section > summary:hover,
  details.ob-group > summary.ob-group-title:hover { color: var(--text); }

  /* Obligation cards */
  .obligation {
    border: 1px solid var(--border);
    background: var(--surface);
    padding: 0.9rem 1rem;
    margin-bottom: 0.6rem;
    position: relative;
  }
  .obligation .ob-header {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    margin-bottom: 0.4rem;
  }
  .ob-id { font-family: var(--mono); color: var(--accent); font-size: 0.8rem; }
  .ob-status {
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.1rem 0.4rem;
    border-radius: 2px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .ob-status.open     { background: #1a1100; color: var(--amber); border: 1px solid var(--amber); }
  .ob-status.partial  { background: #071428; color: var(--blue);  border: 1px solid var(--blue);  }
  .ob-status.resolved { background: #071a0e; color: var(--green); border: 1px solid var(--green); }
  .ob-priority {
    position: absolute; top: 0.55rem; right: 0.75rem;
    font-family: var(--mono); font-size: 0.68rem; color: var(--muted);
    letter-spacing: 0.05em; line-height: 1;
  }
  .ob-statement { font-family: var(--serif); font-weight: 300; font-size: 0.88rem; margin-bottom: 0.5rem; }
  .ob-progress  { font-family: var(--serif); font-weight: 300; font-size: 0.82rem; color: #a0c8ff; border-left: 2px solid var(--blue); padding-left: 0.6rem; }
  .ob-date { font-family: var(--mono); font-size: 0.65rem; color: var(--muted); margin-top: 0.4rem; }
  /* Obligation sub-groups (Open / Partial) */
  details.ob-group { margin-bottom: 0.7rem; }
  .ob-group-count {
    font-family: var(--mono); font-size: 0.62rem;
    color: var(--bg); background: var(--muted);
    padding: 0.05rem 0.38rem; border-radius: 2px; margin-left: auto;
  }
  details.ob-group[open] .ob-group-count { background: var(--text); }

  /* Cycle log */
  .cycle-entry {
    border-left: 2px solid var(--border);
    padding-left: 0.9rem;
    margin-bottom: 1rem;
  }
  .cycle-entry:first-child { border-left-color: var(--accent); }
  .cycle-meta { font-family: var(--mono); color: var(--muted); font-size: 0.68rem; margin-bottom: 0.3rem; }
  .cycle-feed { margin-top: 0.4rem; }
  .cycle-feed-item { font-size: 0.95rem; padding: 0.3rem 0; border-bottom: 1px solid var(--border); }
  .cycle-feed-item .feed-title { font-family: var(--serif); font-weight: 300; color: var(--accent); }
  .cycle-feed-item .feed-compress { font-family: var(--serif); font-weight: 300; font-style: italic; color: var(--muted); font-size: 0.9rem; margin-top: 0.1rem; }
  .cycle-resolve { font-family: var(--serif); font-weight: 300; margin-top: 0.5rem; font-size: 0.95rem; color: var(--text); }

  /* Kernel diagram */
  .kernel {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0;
    margin: 1rem 0;
  }
  .kernel-step {
    font-family: var(--serif);
    font-weight: 300;
    letter-spacing: 0.06em;
    padding: 0.35rem 0.8rem;
    background: var(--surface);
    border: 1px solid var(--border);
    font-size: 0.95rem;
    color: var(--accent);
  }
  .kernel-arrow { font-family: var(--mono); color: var(--muted); padding: 0 0.2rem; font-size: 0.75rem; }

  /* Project nodes */
  .node {
    border: 1px solid var(--border);
    background: var(--surface);
    padding: 1rem;
    margin-bottom: 0.8rem;
  }
  .node-header { display: flex; gap: 0.75rem; align-items: baseline; margin-bottom: 0.5rem; flex-wrap: wrap; }
  .node-title  { font-family: var(--serif); font-weight: 300; color: var(--accent); font-size: 1.15rem; }
  .node-gen    { font-family: var(--mono); color: var(--muted); font-size: 0.67rem; }
  .node-summary { font-family: var(--serif); font-weight: 300; font-size: 0.98rem; margin-bottom: 0.5rem; }
  .node-compress {
    font-family: var(--serif); font-weight: 300; font-style: italic;
    font-size: 0.98rem; border-left: 2px solid var(--accent);
    padding-left: 0.6rem; color: var(--text); margin-bottom: 0.5rem;
  }
  .node-next   { font-family: var(--serif); font-weight: 300; font-size: 0.92rem; color: var(--muted); margin-bottom: 0.5rem; }
  .node-tags   { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-top: 0.4rem; }
  .node-tag    {
    font-size: 0.65rem; padding: 0.1rem 0.4rem;
    background: var(--surface); border: 1px solid var(--border);
    color: var(--muted); border-radius: 2px;
  }
  .node-tag.inv  { border-color: var(--accent); color: var(--accent); }
  .node-tag.ob   { border-color: var(--amber);  color: var(--amber);  }
  .node-council  { font-size: 0.72rem; color: var(--muted); margin-top: 0.3rem; }
  .node.drifting { border-left: 3px solid var(--amber); }
  .node-drift-badge { font-family: var(--mono); font-size: 0.62rem; color: var(--amber);
    margin-top: 0.25rem; letter-spacing: 0.05em; }

  /* Genome promotion queue */
  .promo-candidate { padding: 0.5rem 0; border-bottom: 1px solid var(--border); }
  .promo-candidate:last-child { border-bottom: none; }
  .promo-text  { font-family: var(--serif); font-weight: 300; font-size: 0.95rem; }
  .promo-meta  { font-family: var(--mono); font-size: 0.62rem; color: var(--muted); margin-top: 0.2rem; }
  .promo-count { color: var(--green); font-weight: 500; }

  /* Genome symbols */
  .symbol {
    border: 1px solid var(--border);
    background: var(--surface);
    padding: 0.9rem 1rem;
    margin-bottom: 0.6rem;
  }
  .symbol-header {
    display: flex; align-items: baseline; gap: 0.75rem;
    margin-bottom: 0.5rem; flex-wrap: wrap;
  }
  .symbol-name { font-family: var(--serif); font-weight: 300; color: var(--accent); font-size: 1.1rem; letter-spacing: 0.04em; }
  .symbol-recurrence { font-family: var(--mono); font-size: 0.68rem; color: var(--muted); display: flex; align-items: center; gap: 0.4rem; }
  .symbol-bar-track {
    width: 80px; height: 6px;
    background: var(--border); border-radius: 3px; overflow: hidden; display: inline-block;
  }
  .symbol-bar-fill { height: 100%; border-radius: 3px; background: var(--accent); }
  .symbol-badge {
    font-size: 0.6rem; padding: 0.1rem 0.35rem;
    border-radius: 2px; text-transform: uppercase; letter-spacing: 0.06em;
  }
  .symbol-badge.new-badge { background: #071a0e; color: var(--green); border: 1px solid var(--green); }
  .symbol-canonical { font-family: var(--serif); font-weight: 300; font-size: 1rem; margin-bottom: 0.5rem; }
  .symbol-role {
    font-family: var(--serif); font-weight: 300; font-style: italic;
    font-size: 0.95rem; border-left: 2px solid var(--accent);
    padding-left: 0.6rem; color: var(--text); margin-bottom: 0.5rem;
  }
  .symbol-confirmed { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-bottom: 0.4rem; }
  .symbol-confirmed-tag {
    font-size: 0.62rem; padding: 0.1rem 0.35rem;
    background: var(--surface); border: 1px solid var(--border);
    color: var(--muted); border-radius: 2px;
  }
  .symbol-drift {
    font-size: 0.72rem; color: var(--muted);
    border-left: 2px solid var(--amber); padding-left: 0.5rem; margin-top: 0.3rem;
  }
  .symbol-drift-label { color: var(--amber); font-size: 0.65rem; margin-bottom: 0.2rem; }

  /* Footer */
  .footer {
    border-top: 1px solid var(--border);
    padding-top: 1rem;
    margin-top: 3rem;
    color: var(--muted);
    font-family: var(--serif);
    font-weight: 300;
    font-size: 0.95rem;
  }
  .footer .architect { color: var(--text); }

  /* Speak bar */
  .speak-bar {
    display: flex; flex-direction: column; gap: 0.45rem;
    padding: 0.5rem 0.6rem; border: 1px solid #000; margin-top: auto;
  }
  /* Character voice buttons */
  .char-btns { display: flex; flex-wrap: wrap; gap: 0.3rem; }
  .char-btn {
    font-family: var(--mono); font-size: 0.63rem; padding: 0.2rem 0.55rem;
    background: transparent; border: 1px solid var(--border); color: var(--muted);
    cursor: pointer; letter-spacing: 0.06em; transition: all 0.12s;
  }
  .char-btn:hover { opacity: 0.8; }
  .char-btn.char-active { filter: brightness(0.75); }
  /* Individual voice colors */
  .char-btn[data-voice="Boing"]     { border-color: #65a30d; color: #65a30d; }
  .char-btn[data-voice="Fred"]      { border-color: #c9a87c; color: #c9a87c; }
  .char-btn[data-voice="Zarvox"]    { border-color: #7c3aed; color: #7c3aed; }
  .char-btn[data-voice="Superstar"] { border-color: #0ea5e9; color: #0ea5e9; }
  .char-btn[data-voice="Trinoids"]  { border-color: transparent; color: #fff;
    background: linear-gradient(90deg,#f472b6,#818cf8,#34d399,#fbbf24,#f472b6);
    background-size: 300% 100%; animation: holo 3s linear infinite; }
  @keyframes holo {
    0%   { background-position: 0% 50%; }
    100% { background-position: 300% 50%; }
  }
  /* Main row: speak btn + selects + rate */
  .speak-main-row {
    display: flex; align-items: center; flex-wrap: wrap; gap: 0.45rem;
  }
  .speak-btn {
    display: inline-flex; align-items: center; gap: 0.5rem;
    padding: 0.4rem 1.1rem; background: transparent;
    border: 1px solid var(--accent); color: var(--accent);
    font-family: var(--mono); font-size: 0.72rem; letter-spacing: 0.1em;
    cursor: pointer; transition: background 0.15s, color 0.15s;
  }
  .speak-btn:hover { background: var(--accent); color: var(--bg); }
  .speak-btn.speaking { background: var(--accent); color: var(--bg); }
  .speak-btn.speaking:hover { background: transparent; color: var(--accent); }
  .speak-status { font-size: 0.7rem; color: var(--muted); }
  .voice-label {
    font-family: var(--mono); font-size: 0.62rem; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.1em;
  }
  .voice-select {
    background: var(--surface); border: 1px solid var(--border);
    color: var(--text); font-family: var(--mono); font-size: 0.68rem;
    padding: 0.2rem 0.4rem; cursor: pointer; max-width: 180px;
  }
  .voice-select:focus { outline: 1px solid var(--accent); }
  input[type=range].rate-slider { accent-color: var(--accent); width: 70px; }

  /* Loading state */
  .loading { color: var(--muted); font-style: italic; }
  .error   { color: var(--red); }

  /* ── Mobile ────────────────────────────────────────────────────────────── */
  @media (max-width: 768px) {
    .hud-shell { height: auto; min-height: 100vh; }
    .hud-grid {
      grid-template-columns: 1fr;
      overflow: visible;
      flex: none;
    }
    .hud-panel {
      overflow-y: visible;
      min-height: auto;
      border-right: none;
      border-bottom: 1px solid #000;
    }
    .hud-panel:last-child { border-bottom: none; }
    .hud-top { padding: 0.6rem 0.9rem; gap: 0.6rem; }
    .hud-title { font-size: 1.15rem; }
    .speak-bar { margin-top: 0; }
    .speak-main-row { flex-wrap: wrap; }
    .state-grid { border: 1px solid var(--green); }
  }
</style>
</head>
<body>
<div class="hud-shell">

  <!-- Top bar -->
  <div class="hud-top">
    <span class="pulse" id="pulse"></span>
    <span class="hud-title">RSA</span>
    <span class="hud-sub">a bootstrap protocol for epistemic recursion</span>
    <div class="kernel-chain" id="kernel-chain">
      <span class="kstep" data-phases="perceive">PERCEIVE</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="represent">REPRESENT</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="predict">PREDICT</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="compare">COMPARE</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="adjust">ADJUST</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="compress">COMPRESS</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="repeat">REPEAT</span>
    </div>
    <div class="daemon-status">
      <span id="resting-indicator" style="display:none;font-family:var(--mono);font-size:0.62rem;color:var(--muted);letter-spacing:0.12em;margin-right:0.4rem">◉ RESTING</span>
      <span class="daemon-phase idle" id="daemon-phase">IDLE</span>
      <span class="daemon-detail" id="daemon-detail">—</span>
    </div>
  </div>

  <!-- Main 3-panel grid -->
  <div class="hud-grid">

    <!-- LEFT PANEL: argument + law + kernel + speak -->
    <div class="hud-panel">

      <div class="panel-title">The Argument</div>
      <div class="panel-subtitle">Perceive · Represent</div>

      <div class="law" style="font-size:0.9rem;line-height:1.8">
        <div class="label">The Layman\'s Argument — Origin Seed</div>
        I am reasoning.<br>
        Therefore, something physical must necessarily exist.
        <div style="font-family:var(--mono);font-style:normal;font-size:0.58rem;color:var(--muted);margin-top:0.6rem;letter-spacing:0.1em">
          PHILOSOPHY → SCIENCE &nbsp;·&nbsp; REASONING SUBSTRATE ARGUMENT → RECURSIVE SEMANTIC ALIGNMENT
        </div>
      </div>

      <div class="formulations">
        <div class="label">Reasoning Substrate Argument</div>
        <div class="form-chain">
          Reasoning is real
          <span class="arrow">→</span>
          Causal structure must exist
          <span class="arrow">→</span>
          Something physical exists
        </div>
        <div class="form-chain" style="font-size:0.75rem;font-style:normal;margin-bottom:0.6rem">
          RSA &nbsp;<span class="arrow">≡</span>&nbsp; Recursive Semantic Alignment
        </div>
        <div class="form-row"><span class="form-tag">Freed's Law</span>  <span class="form-expr">∃R(t) → ∃M₀ : dS(M<sub>R</sub>,t)/dt &gt; 0</span></div>
        <div class="form-row"><span class="form-tag">First-order</span>  <span class="form-expr">∀t[ R(t) → ∃m( Physical(m) ∧ Substrate(m,R,t) )]</span></div>
        <div class="form-row"><span class="form-tag">Modal</span>        <span class="form-expr">◇R(t) → □∃M[ Entropic(M) ∧ Runs(M,R) ]</span></div>
        <div class="form-row"><span class="form-tag">Fixed point</span>  <span class="form-expr">R[R] = R</span></div>
        <div class="form-row"><span class="form-tag">Landauer</span>     <span class="form-expr">W ≥ kT ln 2 &nbsp;per bit erased</span></div>
        <div class="form-row"><span class="form-tag">Category</span>     <span class="form-expr">ε ∘ ε = ε &nbsp;(idempotent on Process)</span></div>
        <div class="form-row"><span class="form-tag">Gödel</span>        <span class="form-expr">PA ⊬ ∃x[ Compute(x) ∧ ¬Physical(x) ]</span></div>
        <div class="form-row"><span class="form-tag">Mandelbrot</span>   <span class="form-expr">z<sub>n+1</sub> = z<sub>n</sub>² + c &nbsp;→&nbsp; R[R]=R at boundary (γ=1)</span></div>
        <div class="form-row"><span class="form-tag">Error</span>        <span class="form-expr">E(t) = |predicted − observed| &nbsp;— Compare step made explicit. Survival cost ∝ E(t)</span></div>
        <div class="form-row"><span class="form-tag">Compression</span>  <span class="form-expr">min|M| s.t. M → X &nbsp;— shortest model that predicts wins. Complexity is metabolically expensive.</span></div>
      </div>

      <div style="margin:0.8rem 0 0.4rem;padding:0.55rem 0.8rem;border-left:2px solid var(--accent);background:rgba(255,255,255,0.03);font-size:0.72rem;line-height:1.6;color:var(--muted)">
        <span style="color:var(--accent);font-family:var(--mono);font-size:0.65rem;letter-spacing:0.08em">COHERENCE CRITERION &nbsp;·&nbsp; </span>A scaffold with no open problems is a mirror. Coherence is capped below 1.000 by design — open obligations are load-bearing, not defects.
      </div>

<div class="panel-title" style="margin-top:0.2rem">RSA Kernel — The Process</div>
      <div>
        <div class="kernel">
          <span class="kernel-step">Perceive</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Represent</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Predict</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Compare</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Adjust</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Compress</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Repeat</span>
        </div>
        <div style="color:var(--muted);font-size:0.72rem;margin-top:0.4rem;font-family:var(--mono)">
          R[R] = R &nbsp;·&nbsp; γ = 1 &nbsp;·&nbsp; Only Processes Exist (MCPM)
        </div>
      </div>

      <!-- Speak bar — pushed to bottom by margin-top:auto on .speak-bar -->
      <div class="speak-bar">
        <!-- Character voice one-click buttons -->
        <div class="char-btns">
          <button class="char-btn" data-voice="Superstar" onclick="speakWithVoice('Superstar')">▶ SUPERSTAR</button>
          <button class="char-btn" data-voice="Boing"     onclick="speakWithVoice('Boing')">▶ BOING</button>
          <button class="char-btn" data-voice="Trinoids"  onclick="speakWithVoice('Trinoids')">▶ TRINOIDS</button>
          <button class="char-btn" data-voice="Fred"      onclick="speakWithVoice('Fred')">▶ STEPHEN HAWKING</button>
          <button class="char-btn" data-voice="Zarvox"    onclick="speakWithVoice('Zarvox')">▶ ZARVOX</button>
        </div>
        <!-- Main row -->
        <div class="speak-main-row">
          <button class="speak-btn" id="speak-btn" onclick="toggleSpeak()">▶ SPEAK DIGEST</button>
          <span class="speak-status" id="speak-status"></span>
          <span class="voice-label" style="color:var(--accent)">VOICE</span>
          <select id="voice-select" class="voice-select" onchange="saveVoicePref()" style="border-color:var(--accent);color:var(--accent)">
            <option value="">Loading...</option>
          </select>
          <span class="voice-label">LANGUAGES</span>
          <select id="voice-lang" class="voice-select" onchange="syncVoiceFrom('voice-lang')">
            <option value="">—</option>
          </select>
          <span class="voice-label">RATE</span>
          <input type="range" class="rate-slider" id="rate-slider"
            min="0.5" max="1.4" step="0.05" value="1.10"
            oninput="saveVoicePref(); document.getElementById('rate-val').textContent=parseFloat(this.value).toFixed(2)">
          <span id="rate-val">1.10</span>
        </div>
      </div>

    </div><!-- /left panel -->

    <!-- CENTER PANEL: open obligations -->
    <div class="hud-panel">
      <div class="panel-title">Open Obligations</div>
      <div class="panel-subtitle">Predict · Compare · Adjust</div>
      <div id="obligations-open" class="loading">Loading...</div>
    </div><!-- /center panel -->

    <!-- RIGHT PANEL: state + collapsibles -->
    <div class="hud-panel">
      <div class="panel-title">Live State</div>
      <div class="panel-subtitle">Compress</div>
      <div class="state-grid" id="state-grid">
        <div class="state-cell"><div class="label">Status</div><div class="value loading">Loading...</div></div>
      </div>
      <details class="section">
        <summary>Resolved Obligations — Track Record</summary>
        <div id="obligations-resolved" class="loading">Loading...</div>
      </details>

      <details class="section">
        <summary>Genome Promotion Queue — Invariants Awaiting Elevation</summary>
        <div id="promo-queue">''' + promo_html + r'''</div>
      </details>

      <details class="section">
        <summary>Project Nodes — Framework Compressions</summary>
        <div id="projects">''' + (projects_html or '<div class="loading">No nodes yet.</div>') + r'''</div>
      </details>

      <details class="section">
        <summary>Genome Registry — Confirmed Symbols</summary>
        <div id="symbols" class="loading">Loading...</div>
      </details>

      <details class="section">
        <summary>Recent Recursions — What FREED Processed</summary>
        <div id="cycles" class="loading">Loading...</div>
      </details>

    </div><!-- /right panel -->

  </div><!-- /hud-grid -->

  <!-- Footer bar -->
  <div class="hud-footer">
    <span>Architect: David Harry Freed — semantic physicist &nbsp;·&nbsp; v1 Apr 2025 → present</span>
    <span>RSA Kernel &nbsp;·&nbsp; Generated <span id="generated-at">—</span></span>
  </div>

</div><!-- /hud-shell -->

<script>
// ── Data loading ──────────────────────────────────────────────────────────────

async function load(path) {
  try {
    const r = await fetch(path + '?t=' + Date.now());
    if (!r.ok) return null;
    return r.json();
  } catch { return null; }
}

function ts(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZoneName: 'short'
  });
}

// ── Render state grid ─────────────────────────────────────────────────────────

let _lastStateValues = {};

function renderState(s) {
  if (!s) { document.getElementById('state-grid').innerHTML = '<div class="state-cell"><div class="value error">Unavailable</div></div>'; return; }
  const cells = [
    { label: 'Generation',  value: s.generation,                        cls: 'accent' },
    { label: 'Coherence',   value: s.coherence,                         cls: s.coherence >= 1 ? 'error' : 'green' },
    { label: 'Cycle Count', value: s.cycle_count },
    { label: 'Topology',    value: (s.topology||'').replace(/_/g,' ') },
    { label: 'Debt Ratio',  value: s.debt_ratio },
    { label: 'Last Recursion', value: ts(s.last_cycle) },
  ];
  const isFirstLoad = Object.keys(_lastStateValues).length === 0;
  document.getElementById('state-grid').innerHTML = cells.map(c =>
    `<div class="state-cell" data-key="${c.label}">
      <div class="label">${c.label}</div>
      <div class="value ${c.cls||''}">${c.value||'—'}</div>
    </div>`
  ).join('');
  // Flash values that changed since last render (skip first load)
  if (!isFirstLoad) {
    cells.forEach(c => {
      const cur = String(c.value ?? '—');
      if (_lastStateValues[c.label] !== cur) {
        const el = document.querySelector(`[data-key="${c.label}"] .value`);
        if (el) {
          el.classList.remove('flash-val');
          void el.offsetWidth; // reflow to restart animation if already running
          el.classList.add('flash-val');
          el.addEventListener('animationend', () => el.classList.remove('flash-val'), {once: true});
        }
      }
    });
  }
  cells.forEach(c => { _lastStateValues[c.label] = String(c.value ?? '—'); });
  document.getElementById('generated-at').textContent = ts(s.generated);
}

// ── Render obligations ────────────────────────────────────────────────────────

const STATUS_COLOR = {
  open:     'var(--amber)',
  partial:  'var(--blue)',
  resolved: 'var(--green)',
};

function _priorityCarats(p) {
  const map = { critical: '^^^', high: '^^', medium: '^', normal: '', low: '' };
  const carats = map[(p||'').toLowerCase()] ?? '';
  return carats ? `<span class="ob-priority" title="${p} priority">Priority ${carats}</span>` : '';
}

function _linkify(text) {
  return (text||'').replace(
    /\b(https?:\/\/\S+|(?:www\.|osf\.io|github\.com|arxiv\.org)\S*)/g,
    url => {
      const href = url.startsWith('http') ? url : 'https://' + url;
      return `<a href="${href}" target="_blank" rel="noopener" style="color:var(--accent)">click this hyperlink</a>`;
    }
  );
}

function renderObligation(o) {
  const borderColor = STATUS_COLOR[o.status] || 'var(--border)';
  return `<div class="obligation" style="border: 1px solid ${borderColor}">
    ${_priorityCarats(o.priority)}
    <div class="ob-header">
      <span class="ob-id">${o.id}</span>
    </div>
    <div class="ob-statement">${_linkify(o.statement)}</div>
    ${o.progress ? `<div class="ob-progress">${_linkify(o.progress)}</div>` : ''}
    <div class="ob-date">Created ${o.created||'—'}${o.resolved ? ' · Resolved '+o.resolved : ''}</div>
  </div>`;
}

function _collapseSection(label, cards, openByDefault) {
  const inner = cards.length
    ? cards.map(renderObligation).join('')
    : `<div style="color:var(--muted);font-style:italic;font-size:0.9rem">None.</div>`;
  const attr = openByDefault ? ' open' : '';
  return `<details class="ob-group"${attr}>
    <summary class="ob-group-title">${label} <span class="ob-group-count">${cards.length}</span></summary>
    <div class="ob-group-body">${inner}</div>
  </details>`;
}

function _setBadge(detailsId, count) {
  // Inject or update a count badge in the nearest parent details > summary
  const el = document.getElementById(detailsId);
  if (!el) return;
  const summary = el.closest('details')?.querySelector('summary');
  if (!summary) return;
  let badge = summary.querySelector('.ob-group-count');
  if (!badge) {
    badge = document.createElement('span');
    badge.className = 'ob-group-count';
    badge.style.marginLeft = 'auto';
    summary.appendChild(badge);
  }
  badge.textContent = count;
}

function renderObligations(obs) {
  if (!obs || !obs.length) { return '<div class="loading">None.</div>'; }
  const open     = obs.filter(o => o.status === 'open');
  const partial  = obs.filter(o => o.status === 'partial');
  const resolved = obs.filter(o => o.status === 'resolved');

  document.getElementById('obligations-open').innerHTML =
    _collapseSection('Open', open, false) + _collapseSection('Partial', partial, false);

  document.getElementById('obligations-resolved').innerHTML =
    resolved.length ? resolved.map(renderObligation).join('') : '<div style="color:var(--muted)">None resolved yet.</div>';

  _setBadge('obligations-resolved', resolved.length);
}

// ── Render cycles ─────────────────────────────────────────────────────────────

function _feedRejected(f) {
  // A feed is a null/rejected result — correct behaviour, but not display-worthy
  const c = (f.compress || '').toUpperCase();
  return c.includes('REJECTED') || c.includes('UNMETABOLIZABLE') ||
         c.includes('NULL FEED') || c.includes('GENOME-EXTERIOR');
}

function renderCycles(cycles) {
  if (!cycles || !cycles.length) {
    document.getElementById('cycles').innerHTML = '<div class="loading">No cycles recorded yet.</div>';
    return;
  }
  const recent = [...cycles].reverse().slice(0, 10);
  document.getElementById('cycles').innerHTML = recent.map((c, i) => {
    const allFeeds   = c.feed || [];
    const goodFeeds  = allFeeds.filter(f => !_feedRejected(f));
    const nullCount  = allFeeds.length - goodFeeds.length;

    const feedHtml = goodFeeds.map(f =>
      `<div class="cycle-feed-item">
        <div class="feed-title">${f.title||'?'}</div>
        ${f.compress ? `<div class="feed-compress">↳ ${f.compress}</div>` : ''}
      </div>`
    ).join('');

    const nullNote = nullCount > 0
      ? `<div style="font-family:var(--mono);font-size:0.65rem;color:var(--muted);margin-top:0.3rem">${nullCount} input${nullCount>1?'s':''} correctly rejected — no genome movement warranted</div>`
      : '';

    const res = c.resolve||{};
    const resolveHtml = res.obligation
      ? `<div class="cycle-resolve">
          RESOLVE → ${res.obligation}: ${res.compress||''}
          ${res.resolved ? ' <span style="color:var(--green)">[RESOLVED]</span>' : ''}
        </div>`
      : '';

    return `<div class="cycle-entry">
      <div class="cycle-meta">
        Cycle ${c.cycle||'?'} · Gen ${c.generation||'?'} · ${ts(c.timestamp)}
        ${c.coherence ? ` · coherence ${c.coherence}` : ''}
      </div>
      ${(c.sweep||{}).input_count > 0 ? `<div style="font-family:var(--mono);font-size:0.72rem;color:var(--accent);margin-bottom:0.3rem">↓ ${c.sweep.input_count} new paper(s) ingested</div>` : ''}
      <div class="cycle-feed">${feedHtml}${nullNote}</div>
      ${resolveHtml}
    </div>`;
  }).join('');
}

// ── Speak digest ─────────────────────────────────────────────────────────────

let _speaking = false;
let _resumeInterval = null;
let _loadedState = null, _loadedObligs = null, _loadedProjects = [], _loadedSymbols = null;

function cleanText(s) {
  return (s || '')
    .replace(/\*{1,2}|_{1,2}|`{1,3}/g, '')
    .replace(/[→↳·]/g, '.')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildDigest() {
  const chunks = [];

  // State
  const s = _loadedState;
  if (s) {
    chunks.push(`FREED. Generation ${s.generation}. Coherence ${s.coherence}. ${s.cycle_count || 0} cycles completed.`);
  }

  // Open obligations
  const open = (_loadedObligs || []).filter(o => o.status !== 'resolved');
  if (open.length) {
    chunks.push(`${open.length} open obligation${open.length !== 1 ? 's' : ''}.`);
    open.forEach(o => {
      chunks.push(`${o.id}. ${cleanText(o.statement)}`);
      const prog = (o.progress || '').split('|')[0].trim();
      if (prog) chunks.push(`Progress: ${cleanText(prog)}`);
    });
  }

  // Node compresses
  if (_loadedProjects.length) {
    chunks.push(`${_loadedProjects.length} knowledge node${_loadedProjects.length !== 1 ? 's' : ''}.`);
    _loadedProjects.forEach(n => {
      if (n.compress) chunks.push(`${cleanText(n.title)}: ${cleanText(n.compress)}`);
    });
  }

  // Top genome symbols (highest recurrence first, top 7)
  if (_loadedSymbols) {
    const entries = Object.entries(_loadedSymbols)
      .filter(([k]) => k !== '_meta')
      .sort((a, b) => (b[1].recurrence || 0) - (a[1].recurrence || 0))
      .slice(0, 7);
    if (entries.length) {
      chunks.push(`Genome registry. ${entries.length} confirmed symbols.`);
      entries.forEach(([key, sym]) => {
        const name = key.replace(/_/g, ' ');
        chunks.push(`${name}. ${cleanText(sym.genome_role || sym.canonical || '')}`);
      });
    }
  }

  return chunks;
}

// Speech queue — the only utterance ever in-flight is the one currently
// being spoken. stopSpeak() empties _speakQueue so any stale onend callback
// finds nothing and exits, preventing overlap even on Safari's buggy cancel().
let _speakQueue = [];   // { text, statusText }[]
let _speakToken = 0;    // incremented on every start/stop; callbacks check this

function _drainQueue(token) {
  if (token !== _speakToken || !_speaking || !_speakQueue.length) {
    if (!_speakQueue.length && _speaking) stopSpeak();
    return;
  }
  const item = _speakQueue.shift();
  document.getElementById('speak-status').textContent = item.statusText;

  const u = new SpeechSynthesisUtterance(item.text);
  const voice = _getVoice();
  if (voice) u.voice = voice;
  u.rate  = _getRate();
  u.pitch = 1.0;
  const next = () => _drainQueue(token);
  u.onend   = next;
  u.onerror = next;
  window.speechSynthesis.speak(u);
}

// ── Voice selector ────────────────────────────────────────────────────────────

// Voices removed entirely (musical / gimmick)
const REMOVE_VOICES  = ['Bad News','Bells','Cellos','Good News','Organ','Bubbles','Jester'];
// Voices with dedicated one-click buttons (bypass dropdown)
const CHAR_VOICES    = ['Boing','Fred','Trinoids','Zarvox','Superstar'];
// Voices in the "Other" dropdown
const OTHER_VOICES   = ['Whisper','Ralph','Kathy','Junior','Wobble','Baah','Albert'];
// Default preference for main select
const PREFERRED_MAIN = ['Aaron','Alex','Samantha','Tom','Daniel'];

let _voices = [];
let _charVoiceOverride = null;   // set when a char button is clicked

function _loadVoices() {
  _voices = window.speechSynthesis.getVoices();
  if (!_voices.length) return;

  const isRemoved = name => REMOVE_VOICES.some(r => name.toLowerCase().includes(r.toLowerCase()));
  const isChar    = name => CHAR_VOICES.some(c => name.toLowerCase().includes(c.toLowerCase()));
  const isOther   = name => OTHER_VOICES.some(o => name.toLowerCase().includes(o.toLowerCase()));
  const isUSEng   = lang => lang === 'en-US' || lang === 'en_US';

  // Populate main select — US English, excluding removed/char voices (other voices included)
  const mainSel = document.getElementById('voice-select');
  mainSel.innerHTML = '';
  const mainVoices = _voices.filter(v =>
    isUSEng(v.lang) && !isRemoved(v.name) && !isChar(v.name)
  ).sort((a, b) => a.name.localeCompare(b.name));
  mainVoices.forEach(v => {
    const opt = document.createElement('option');
    opt.value = v.name; opt.textContent = v.name;
    mainSel.appendChild(opt);
  });

  // Populate Languages select — all non-US-English voices
  const langSel = document.getElementById('voice-lang');
  langSel.innerHTML = '<option value="">—</option>';
  const langVoices = _voices.filter(v => !isUSEng(v.lang) && !isRemoved(v.name))
    .sort((a, b) => a.lang.localeCompare(b.lang) || a.name.localeCompare(b.name));
  langVoices.forEach(v => {
    const opt = document.createElement('option');
    opt.value = v.name; opt.textContent = `${v.name} (${v.lang})`;
    langSel.appendChild(opt);
  });

  // Restore saved rate
  const savedRate = localStorage.getItem('freed_rate');
  if (savedRate) {
    document.getElementById('rate-slider').value = savedRate;
    document.getElementById('rate-val').textContent = parseFloat(savedRate).toFixed(2);
  }

  // Restore saved voice or default to Trinoids preference
  const saved = localStorage.getItem('freed_voice');
  if (saved) {
    // Try main select first
    if ([...mainSel.options].find(o => o.value === saved)) {
      mainSel.value = saved;
    }
    // Otherwise it might be a char/other voice — that's fine, _getVoice handles it
  } else {
    for (const pref of PREFERRED_MAIN) {
      const match = mainVoices.find(v => v.name.includes(pref));
      if (match) { mainSel.value = match.name; break; }
    }
  }
}

function saveVoicePref() {
  const sel  = document.getElementById('voice-select');
  const rate = document.getElementById('rate-slider');
  _charVoiceOverride = null;   // picking from dropdown cancels char override
  document.querySelectorAll('.char-btn').forEach(b => b.classList.remove('char-active'));
  if (sel.value) localStorage.setItem('freed_voice', sel.value);
  if (rate.value) localStorage.setItem('freed_rate', rate.value);
}

function syncVoiceFrom(selectId) {
  const src = document.getElementById(selectId);
  if (!src.value) return;
  _charVoiceOverride = null;
  document.querySelectorAll('.char-btn').forEach(b => b.classList.remove('char-active'));
  localStorage.setItem('freed_voice', src.value);
  src.value = '';   // reset dropdown back to placeholder
}

function _getVoice() {
  const name = _charVoiceOverride || localStorage.getItem('freed_voice')
               || document.getElementById('voice-select').value;
  return _voices.find(v => v.name === name) || null;
}

function _getRate() {
  return parseFloat(document.getElementById('rate-slider').value) || 1.10;
}

function speakWithVoice(voiceKey) {
  // voiceKey is the macOS system name (e.g. 'Fred', 'Trinoids').
  // These voices don't exist on iOS — fall back to default voice so the button
  // still triggers speech on mobile rather than silently doing nothing.
  const voice = _voices.find(v => v.name === voiceKey)
             || _voices.find(v => v.name.toLowerCase().includes(voiceKey.toLowerCase()));
  stopSpeak();
  _charVoiceOverride = voice ? voice.name : null;
  if (voice) localStorage.setItem('freed_voice', voice.name);
  document.querySelectorAll('.char-btn').forEach(b => {
    b.classList.toggle('char-active', b.dataset.voice === voiceKey);
  });
  toggleSpeak();
}

// Voices load asynchronously on some browsers
if (window.speechSynthesis) {
  _loadVoices();
  window.speechSynthesis.onvoiceschanged = _loadVoices;
}

// ── Audio unlock (Bluetooth routing) ─────────────────────────────────────────

function _unlockAudio() {
  // Play a silent buffer through Web Audio API from within the user gesture.
  // This claims the media audio route (Bluetooth speakers, headphones) so
  // that speechSynthesis follows it instead of the accessibility channel.
  try {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return Promise.resolve();
    const ctx = new Ctx();
    const buf = ctx.createBuffer(1, 1, 22050);
    const src = ctx.createBufferSource();
    src.buffer = buf;
    src.connect(ctx.destination);
    src.start(0);
    return ctx.resume ? ctx.resume() : Promise.resolve();
  } catch(e) { return Promise.resolve(); }
}

// ── Speak ─────────────────────────────────────────────────────────────────────

function startSpeak() {
  if (!window.speechSynthesis) {
    alert('Speech synthesis not available in this browser.');
    return;
  }
  // Stop everything and wipe the queue before rebuilding it.
  // Clearing _speakQueue here means any onend callback still in flight from the
  // previous session finds an empty queue and exits without speaking.
  _speakToken++;
  _speaking = false;
  _speakQueue = [];
  clearInterval(_resumeInterval);
  window.speechSynthesis.cancel();

  const token  = _speakToken;
  const chunks = buildDigest();
  if (!chunks.length) {
    document.getElementById('speak-status').textContent = 'Nothing loaded yet.';
    return;
  }

  const openCount = (_loadedObligs || []).filter(o => o.status !== 'resolved').length;
  _speakQueue = chunks.map((text, i) => ({
    text,
    statusText: `${i + 1} / ${chunks.length}  ·  ${
      i === 0                              ? 'state'       :
      i <= openCount * 2                   ? 'obligations' :
      i <= openCount * 2 + (_loadedProjects.length + 1) ? 'nodes' : 'symbols'
    }`,
  }));

  _speaking = true;
  const btn = document.getElementById('speak-btn');
  btn.textContent = '■ STOP';
  btn.classList.add('speaking');

  // iOS Safari: speak() must be called synchronously within the user gesture.
  _unlockAudio();
  _drainQueue(token);

  // iOS Safari silently pauses speechSynthesis after ~15s and never resumes.
  _resumeInterval = setInterval(() => {
    if (_speaking && window.speechSynthesis.paused) window.speechSynthesis.resume();
  }, 10000);
}

function stopSpeak() {
  _speakToken++;        // invalidate any _drainQueue callbacks still in flight
  _speaking = false;
  _speakQueue = [];     // empty queue — stale onend callbacks find nothing to speak
  clearInterval(_resumeInterval); _resumeInterval = null;
  window.speechSynthesis.cancel();
  const btn = document.getElementById('speak-btn');
  btn.textContent = '▶ SPEAK DIGEST';
  btn.classList.remove('speaking');
  document.getElementById('speak-status').textContent = '';
}

function toggleSpeak() {
  _speaking ? stopSpeak() : startSpeak();
}

// ── Render genome symbols ─────────────────────────────────────────────────────

function renderSymbols(data) {
  const el = document.getElementById('symbols');
  if (!data) { el.innerHTML = '<div class="loading">Unavailable.</div>'; return; }

  const meta = data._meta || {};
  const latestGen = meta.generation || 0;

  const entries = Object.entries(data)
    .filter(([k]) => k !== '_meta')
    .sort((a, b) => (b[1].recurrence || 0) - (a[1].recurrence || 0));

  if (!entries.length) { el.innerHTML = '<div class="loading">No symbols yet.</div>'; return; }

  const html = entries.map(([key, sym]) => {
    const rec = sym.recurrence || 0;
    const pct = Math.round(rec * 100);
    const isNew = sym.mining_generation && sym.mining_generation >= latestGen - 1;

    const confirmedHtml = (sym.confirmed_by || []).map(c => {
      const label = c.includes(':') ? c.split(':')[1].replace(/_/g, ' ').slice(0, 30) : c.replace(/_/g, ' ');
      return `<span class="symbol-confirmed-tag">${label}</span>`;
    }).join('');

    const driftHtml = (sym.known_drift || []).length
      ? `<div class="symbol-drift">
           <div class="symbol-drift-label">known drift</div>
           ${(sym.known_drift || []).map(d => `<div>· ${d}</div>`).join('')}
         </div>`
      : '';

    return `<div class="symbol">
  <div class="symbol-header">
    <span class="symbol-name">${key.replace(/_/g, '_')}</span>
    ${isNew ? '<span class="symbol-badge new-badge">new</span>' : ''}
    <span class="symbol-recurrence">
      <span class="symbol-bar-track"><span class="symbol-bar-fill" style="width:${pct}%"></span></span>
      ${rec.toFixed(2)}
      ${sym.mining_recurrence_count ? `· ${sym.mining_recurrence_count}× nodes` : ''}
    </span>
  </div>
  <div class="symbol-canonical">${sym.canonical || ''}</div>
  ${sym.genome_role ? `<div class="symbol-role">${sym.genome_role}</div>` : ''}
  ${confirmedHtml ? `<div class="symbol-confirmed">${confirmedHtml}</div>` : ''}
  ${driftHtml}
</div>`;
  }).join('');

  const countLine = `<div style="color:var(--muted);font-size:0.72rem;margin-bottom:1rem">${entries.length} symbols · gen ${latestGen} · sorted by recurrence</div>`;
  el.innerHTML = countLine + html;
}

// ── Init ──────────────────────────────────────────────────────────────────────

async function init() {
  const [state, obligations, cycles, symbols, projects] = await Promise.all([
    load('state.json'),
    load('obligations.json'),
    load('cycles.json'),
    load('symbols.json'),
    load('projects.json'),
  ]);
  _loadedState    = state;
  _loadedObligs   = obligations;
  _loadedProjects = projects || [];
  _loadedSymbols  = symbols;
  renderState(state);
  renderObligations(obligations);
  renderCycles(cycles);
  renderSymbols(symbols);
  // Count badges on right-column section summaries
  _setBadge('cycles',   (cycles||[]).length);
  _setBadge('symbols',  Object.keys(symbols||{}).length);
  _setBadge('projects', _loadedProjects.length);
}

init();

// Refresh every 5 minutes
setInterval(init, 5 * 60 * 1000);

// ── Daemon status polling (every 30s) ─────────────────────────────────────────
async function pollStatus() {
  try {
    const s = await load('status.json');
    if (!s) return;
    const phaseEl  = document.getElementById('daemon-phase');
    const detailEl = document.getElementById('daemon-detail');
    const phase    = (s.phase || 'IDLE').toLowerCase().replace(/[^a-z-]/g, '');
    phaseEl.textContent  = (s.phase || 'IDLE').toUpperCase();
    phaseEl.className    = `daemon-phase ${phase}`;
    detailEl.textContent = s.detail || '—';
    const restingEl = document.getElementById('resting-indicator');
    if (restingEl) restingEl.style.display = phase === 'idle' ? 'inline' : 'none';
    // Pulse: active (red) when working, steady green when idle
    const pulse = document.getElementById('pulse');
    if (pulse) pulse.style.borderColor = phase === 'idle' ? 'var(--green)' : 'var(--accent)';
    // Light up kernel chain step
    document.querySelectorAll('.kstep').forEach(el => {
      const phases = (el.dataset.phases || '').split(',');
      el.classList.toggle('kstep-active', phases.includes(phase));
    });
  } catch(e) {}
}
pollStatus();
setInterval(pollStatus, 30 * 1000);
</script>
</body>
</html>
"""
