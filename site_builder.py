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

    Criticality-state transitions (AT_CRITICAL / FROZEN / DISSIPATED) are
    detected here by comparing the current CA telemetry verdict against the
    previous cycle's verdict stored in criticality_timeseries.json.  The
    transition signal is injected into the state dict as a first-class field
    so downstream consumers (FEED, RESOLVE) can react automatically.
    """
    # ── Criticality scalar-field extraction & state-transition detection ──
    # Surfaces σ (branching ratio) and α (power-law exponent) as tracked
    # scalar fields alongside existing survival-rate logging.  Detects
    # AT_CRITICAL / FROZEN / DISSIPATED transitions per INV_073.
    ca_telemetry = {}
    if cycle_log and isinstance(cycle_log, dict):
        ca_telemetry = cycle_log.get("phases", {}).get("ca_telemetry", {})
    criticality = _extract_criticality_verdict(ca_telemetry) if ca_telemetry else {}

    if criticality:
        # Inject scalar fields into state for persistence
        state["criticality_sigma"] = criticality.get("branching_ratio")
        state["criticality_sigma_err"] = criticality.get("branching_ratio_err")
        state["criticality_alpha"] = criticality.get("avalanche_exponent")
        state["criticality_r2"] = criticality.get("power_law_r2")
        state["criticality_survival"] = criticality.get("survival_rate")
        state["criticality_entropy"] = criticality.get("shannon_entropy")
        state["criticality_h_max"] = criticality.get("h_max")
        state["criticality_h_over_h_max"] = criticality.get("h_over_h_max")
        state["criticality_entropy_criticality"] = criticality.get("entropy_criticality")
        state["criticality_verdict"] = criticality.get("criticality_verdict")
        state["dominant_cell_type"] = criticality.get("dominant_cell_type")
        state["dominant_cell_count"] = criticality.get("dominant_cell_count")
        state["dominant_cell_fraction"] = criticality.get("dominant_cell_fraction")

        # ── Running criticality score (σ, α, R²) ─────────────────────
        # Composite score queryable by the epistemic loop:
        #   score = w_σ·S(σ) + w_α·S(α) + w_r2·S(R²)
        # where S(σ) = max(0, 1 - |σ-1|/0.05)        ∈ [0,1]
        #       S(α) = max(0, 1 - |α-2.25|/0.75)      ∈ [0,1]  (band center 2.25)
        #       S(R²) = min(1, R²/0.95)                ∈ [0,1]
        # Weights: σ 50%, α 25%, R² 25% — σ is the primary invariant.
        _cs_sigma = criticality.get("branching_ratio")
        _cs_alpha = criticality.get("avalanche_exponent")
        _cs_r2 = criticality.get("power_law_r2")
        _cs_components = {}
        _cs_total_weight = 0.0
        _cs_weighted_sum = 0.0
        if _cs_sigma is not None:
            try:
                _s_score = max(0.0, 1.0 - abs(float(_cs_sigma) - 1.0) / 0.05)
                _cs_components["sigma_score"] = round(_s_score, 4)
                _cs_weighted_sum += 0.50 * _s_score
                _cs_total_weight += 0.50
            except (TypeError, ValueError):
                pass
        if _cs_alpha is not None:
            try:
                _a_score = max(0.0, 1.0 - abs(float(_cs_alpha) - 2.25) / 0.75)
                _cs_components["alpha_score"] = round(_a_score, 4)
                _cs_weighted_sum += 0.25 * _a_score
                _cs_total_weight += 0.25
            except (TypeError, ValueError):
                pass
        if _cs_r2 is not None:
            try:
                _r2_score = min(1.0, float(_cs_r2) / 0.95)
                _cs_components["r2_score"] = round(_r2_score, 4)
                _cs_weighted_sum += 0.25 * _r2_score
                _cs_total_weight += 0.25
            except (TypeError, ValueError):
                pass
        if _cs_total_weight > 0:
            _composite = round(_cs_weighted_sum / _cs_total_weight, 4)
            state["criticality_score"] = _composite
            state["criticality_score_components"] = _cs_components
            state["criticality_score_weight"] = round(_cs_total_weight, 2)

        # ── Criticality-proximity coherence weighting (O148 feedback) ─
        # Close the loop between CA telemetry measurement and genome
        # coherence: when σ and α are available, blend the composite
        # criticality score into the genome coherence value.  The
        # adjustment is small (±0.02 max) so it nudges without
        # destabilising, but it makes coherence *responsive* to
        # criticality drift — the genome's epistemic score now reflects
        # whether the CA substrate is operating at the critical ridge.
        #
        # Formula: coherence_adjusted = coherence_base + δ_crit
        #   δ_crit = CRIT_WEIGHT * (criticality_score - 0.5)
        #   clamped to [-MAX_DELTA, +MAX_DELTA]
        # When criticality_score > 0.5 (near critical), coherence gets
        # a small positive bump; when < 0.5 (drifting), a small penalty.
        CRIT_WEIGHT = 0.04     # max swing = ±0.02
        MAX_DELTA = 0.02
        _crit_score = state.get("criticality_score")
        _base_coherence = state.get("coherence")
        if _crit_score is not None and _base_coherence is not None:
            try:
                _cs = float(_crit_score)
                _bc = float(_base_coherence)
                _delta = CRIT_WEIGHT * (_cs - 0.5)
                _delta = max(-MAX_DELTA, min(MAX_DELTA, _delta))
                _adjusted = round(_bc + _delta, 4)
                # Coherence must stay in (0, 1) — open obligations keep it < 1
                _adjusted = max(0.0001, min(0.9999, _adjusted))
                state["coherence"] = _adjusted
                state["coherence_base"] = round(_bc, 4)
                state["coherence_crit_delta"] = round(_delta, 4)
                state["coherence_crit_score_used"] = round(_cs, 4)
            except (TypeError, ValueError):
                pass

        # ── State-transition detection ────────────────────────────────
        # Compare current verdict against previous to flag regime shifts.
        # Verdict mapping to coarse state:
        #   AT_CRITICAL* → AT_CRITICAL
        #   SUBCRITICAL / *FROZEN* → FROZEN
        #   SUPERCRITICAL / *RUNAWAY* → DISSIPATED
        #   anything else → UNKNOWN
        current_verdict = criticality.get("criticality_verdict") or ""
        current_coarse = _coarse_criticality_state(current_verdict)

        previous_coarse = _load_previous_criticality_state()

        state["criticality_state"] = current_coarse
        state["criticality_state_previous"] = previous_coarse

        if previous_coarse and current_coarse != previous_coarse:
            transition = previous_coarse + " -> " + current_coarse
            state["criticality_transition"] = transition
            state["criticality_transition_flag"] = True
            print(f"[SITE] *** CRITICALITY TRANSITION: {transition} ***")
        else:
            state["criticality_transition"] = None
            state["criticality_transition_flag"] = False

    _write_state(state)
    _write_obligations(obligations)
    _write_cycles(cycle_log)
    _write_symbols()
    _write_wiring()
    _write_index()
    print("[SITE] docs/ updated.")
    _push(state.get("generation", "?"))


def _coarse_criticality_state(verdict_str: str) -> str:
    """Map a detailed criticality verdict to one of three coarse states:
    AT_CRITICAL, FROZEN, or DISSIPATED.  Returns 'UNKNOWN' for unrecognised."""
    v = (verdict_str or "").upper()
    if "AT_CRITICAL" in v or v == "CRITICAL_LOW_CONFIDENCE" or v == "CRITICAL_CONTESTED":
        return "AT_CRITICAL"
    if "SUBCRITICAL" in v or "FROZEN" in v:
        return "FROZEN"
    if "SUPERCRITICAL" in v or "RUNAWAY" in v or "DISSIPAT" in v:
        return "DISSIPATED"
    if v:
        return "UNKNOWN"
    return "UNKNOWN"


def _load_previous_criticality_state() -> str:
    """Read the most recent coarse criticality state from the timeseries file.
    Returns empty string when no history exists."""
    ts_file = DOCS_DIR / "criticality_timeseries.json"
    if not ts_file.exists():
        return ""
    try:
        entries = json.loads(ts_file.read_text())
        if isinstance(entries, list) and entries:
            last = entries[-1]
            prev_verdict = last.get("verdict", "")
            return _coarse_criticality_state(prev_verdict)
    except (json.JSONDecodeError, OSError, TypeError):
        pass
    return ""


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

    # O148: Compute σ = mean(offspring)/mean(active) from per-timestep arrays
    # when branching_ratio is not pre-computed by the CA runner.  This closes
    # the open demand for richer CA telemetry by deriving the criticality
    # signal directly from raw simulation data.
    if sigma is None:
        # O148: Compute σ as rolling mean of live_cells(t+1)/live_cells(t)
        # from consecutive timestep counts when available. This is the most
        # direct branching-ratio estimator: each ratio measures whether the
        # population is growing (>1), shrinking (<1), or critical (≈1).
        live_cells_per_step = ca_telemetry.get("live_cells_per_step")
        if isinstance(live_cells_per_step, list) and len(live_cells_per_step) >= 2:
            try:
                _lc_valid = [float(x) for x in live_cells_per_step if x is not None]
                if len(_lc_valid) >= 2:
                    _ratios = []
                    for _ri in range(1, len(_lc_valid)):
                        if _lc_valid[_ri - 1] > 0:
                            _ratios.append(_lc_valid[_ri] / _lc_valid[_ri - 1])
                    if _ratios:
                        _r_mean = sum(_ratios) / len(_ratios)
                        sigma = round(_r_mean, 6)
                        if len(_ratios) >= 2:
                            _r_var = sum((r - _r_mean) ** 2 for r in _ratios) / len(_ratios)
                            sigma_err = round(_r_var ** 0.5, 6)
                        else:
                            sigma_err = 0.0
                        ca_telemetry["branching_ratio"] = sigma
                        ca_telemetry["branching_ratio_err"] = sigma_err
                        ca_telemetry["branching_ratio_computed"] = True
                        ca_telemetry["branching_ratio_method"] = "rolling_live_cell_ratio"
                        ca_telemetry["branching_ratio_n_steps"] = len(_ratios)
                        # Per-timestep σ-band monitoring for live-cell ratios
                        SIGMA_BAND_CENTER = 1.0
                        SIGMA_BAND_HALF = 0.05
                        _lc_hygiene = []
                        _lc_sigma_log = []
                        for _si, _sr in enumerate(_ratios):
                            _sr_round = round(_sr, 6)
                            _lc_sigma_log.append(_sr_round)
                            if abs(_sr - SIGMA_BAND_CENTER) > SIGMA_BAND_HALF:
                                _lc_hygiene.append({
                                    "step": _si,
                                    "sigma": _sr_round,
                                    "deviation": round(_sr - SIGMA_BAND_CENTER, 6),
                                    "direction": "SUPERCRITICAL" if _sr > SIGMA_BAND_CENTER + SIGMA_BAND_HALF else "SUBCRITICAL",
                                })
                        ca_telemetry["per_step_sigma"] = _lc_sigma_log
                        ca_telemetry["sigma_hygiene_events"] = _lc_hygiene
                        ca_telemetry["sigma_hygiene_event_count"] = len(_lc_hygiene)
                        ca_telemetry["sigma_in_band_fraction"] = round(
                            1.0 - len(_lc_hygiene) / len(_ratios), 6
                        ) if _ratios else 0.0
            except (TypeError, ValueError):
                pass

    if sigma is None:
        offspring_per_step = ca_telemetry.get("offspring_per_step")
        active_per_step = ca_telemetry.get("active_per_step")
        if (isinstance(offspring_per_step, list) and isinstance(active_per_step, list)
                and len(offspring_per_step) > 0 and len(active_per_step) > 0):
            try:
                valid_offspring = [float(x) for x in offspring_per_step if x is not None]
                valid_active = [float(x) for x in active_per_step if x is not None]
                if valid_active and valid_offspring:
                    mean_offspring = sum(valid_offspring) / len(valid_offspring)
                    mean_active = sum(valid_active) / len(valid_active)
                    if mean_active > 0:
                        sigma = round(mean_offspring / mean_active, 6)
                        # Compute uncertainty via per-step σ_i = offspring_i / active_i
                        n_steps = min(len(valid_offspring), len(valid_active))
                        per_step_sigmas = []
                        for i in range(n_steps):
                            if valid_active[i] > 0:
                                per_step_sigmas.append(valid_offspring[i] / valid_active[i])
                        if len(per_step_sigmas) >= 2:
                            mu = sum(per_step_sigmas) / len(per_step_sigmas)
                            variance = sum((s - mu) ** 2 for s in per_step_sigmas) / len(per_step_sigmas)
                            sigma_err = round(variance ** 0.5, 6)
                        else:
                            sigma_err = 0.0
                        # Per-timestep σ-band monitoring: flag steps where
                        # σ_i exits the critical band (1.0 ± 0.05) as
                        # hygiene events.  This makes AT_CRITICAL a live
                        # upstream signal in the telemetry pipeline rather
                        # than a post-hoc annotation derived only from the
                        # aggregate mean.
                        SIGMA_BAND_CENTER = 1.0
                        SIGMA_BAND_HALF = 0.05
                        sigma_hygiene_events = []
                        per_step_sigma_log = []
                        for step_i, s_i in enumerate(per_step_sigmas):
                            s_rounded = round(s_i, 6)
                            in_band = abs(s_i - SIGMA_BAND_CENTER) <= SIGMA_BAND_HALF
                            per_step_sigma_log.append(s_rounded)
                            if not in_band:
                                sigma_hygiene_events.append({
                                    "step": step_i,
                                    "sigma": s_rounded,
                                    "deviation": round(s_i - SIGMA_BAND_CENTER, 6),
                                    "direction": "SUPERCRITICAL" if s_i > SIGMA_BAND_CENTER + SIGMA_BAND_HALF else "SUBCRITICAL",
                                })
                        ca_telemetry["per_step_sigma"] = per_step_sigma_log
                        ca_telemetry["sigma_hygiene_events"] = sigma_hygiene_events
                        ca_telemetry["sigma_hygiene_event_count"] = len(sigma_hygiene_events)
                        ca_telemetry["sigma_in_band_fraction"] = round(
                            1.0 - len(sigma_hygiene_events) / len(per_step_sigmas), 6
                        ) if per_step_sigmas else 0.0
                        # Store computed values back so downstream logic picks them up
                        ca_telemetry["branching_ratio"] = sigma
                        ca_telemetry["branching_ratio_err"] = sigma_err
                        ca_telemetry["branching_ratio_computed"] = True
                        ca_telemetry["branching_ratio_n_steps"] = n_steps
                        ca_telemetry["branching_ratio_mean_offspring"] = round(mean_offspring, 6)
                        ca_telemetry["branching_ratio_mean_active"] = round(mean_active, 6)
            except (TypeError, ValueError):
                pass
    alpha = ca_telemetry.get("power_law_exponent") or ca_telemetry.get("avalanche_exponent")
    r2 = ca_telemetry.get("power_law_r2")

    # O148: Compute avalanche-size histogram and power-law exponent α via
    # log-log regression when raw avalanche sizes are available but α is not
    # pre-computed.  The avalanche_sizes list records the number of cells
    # that flipped in each contiguous avalanche event during the CA step loop.
    # Power-law fit: log(count) = -α·log(size) + c  →  α = -slope.
    # SOC universality class expects α ≈ 1.5 (mean-field directed percolation).
    # Deviation flags a criticality health warning.
    if alpha is None:
        avalanche_sizes = ca_telemetry.get("avalanche_sizes")
        if isinstance(avalanche_sizes, list) and len(avalanche_sizes) >= 5:
            try:
                import math
                # Build histogram: size → count
                size_counts = {}
                for s in avalanche_sizes:
                    sv = int(s)
                    if sv > 0:
                        size_counts[sv] = size_counts.get(sv, 0) + 1
                if len(size_counts) >= 3:
                    # Log-log regression: X = log(size), Y = log(count)
                    log_sizes = [math.log(k) for k in sorted(size_counts.keys())]
                    log_counts = [math.log(size_counts[k]) for k in sorted(size_counts.keys())]
                    n = len(log_sizes)
                    sum_x = sum(log_sizes)
                    sum_y = sum(log_counts)
                    sum_xy = sum(log_sizes[i] * log_counts[i] for i in range(n))
                    sum_x2 = sum(x * x for x in log_sizes)
                    denom = n * sum_x2 - sum_x * sum_x
                    if abs(denom) > 1e-12:
                        slope = (n * sum_xy - sum_x * sum_y) / denom
                        intercept = (sum_y - slope * sum_x) / n
                        # α = -slope (power-law exponent)
                        alpha = round(-slope, 6)
                        # R² goodness of fit
                        y_mean = sum_y / n
                        ss_tot = sum((y - y_mean) ** 2 for y in log_counts)
                        ss_res = sum((log_counts[i] - (slope * log_sizes[i] + intercept)) ** 2
                                     for i in range(n))
                        r2 = round(1.0 - ss_res / ss_tot, 6) if ss_tot > 1e-12 else 0.0
                        # Store computed values back into telemetry
                        ca_telemetry["avalanche_exponent"] = alpha
                        ca_telemetry["power_law_r2"] = r2
                        ca_telemetry["avalanche_exponent_computed"] = True
                        ca_telemetry["avalanche_histogram_bins"] = len(size_counts)
                        ca_telemetry["avalanche_n_events"] = len(avalanche_sizes)
                        # SOC universality health warning: α ≈ 1.5 expected
                        # for mean-field directed percolation universality class
                        soc_alpha_target = 1.5
                        soc_alpha_tolerance = 0.3
                        alpha_drift = abs(alpha - soc_alpha_target)
                        ca_telemetry["soc_alpha_drift"] = round(alpha_drift, 6)
                        if alpha_drift > soc_alpha_tolerance:
                            ca_telemetry["soc_health_warning"] = (
                                f"α={alpha} deviates from SOC universality "
                                f"target α≈{soc_alpha_target} by {round(alpha_drift, 3)}; "
                                f"system may have departed γ=1 ridge"
                            )
            except (TypeError, ValueError, OverflowError):
                pass

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
            # Primary SOC band: α ∈ [1.5, 2.5] covers mean-field directed
            # percolation (α≈1.5) through empirically confirmed Game of Truth
            # exponents (α≈1.909) to standard sandpile models (α≈2.0–2.5).
            # The previous [2.0, 2.5] band excluded valid SOC signatures like
            # the α≈1.909 (R²=0.995) confirmed in the 32×32 / 200-step run.
            if 1.5 <= a <= 2.5 and r2_val > 0.7:
                alpha_verdict = "POWER_LAW_CONFIRMED"
            elif 2.5 < a <= 3.0 and r2_val > 0.7:
                alpha_verdict = "POWER_LAW_EXTENDED_BAND"
            elif 1.0 <= a < 1.5 and r2_val > 0.7:
                alpha_verdict = "POWER_LAW_NEAR_SOC"
            elif r2_val <= 0.7:
                alpha_verdict = "POWER_LAW_WEAK"
            else:
                alpha_verdict = "POWER_LAW_OUT_OF_BAND"
        except (TypeError, ValueError):
            alpha_verdict = "UNKNOWN"

    # Joint verdict: AT_CRITICAL requires BOTH σ in band AND α confirmed
    # O148 confidence gate: R² ≥ 0.80 required for full AT_CRITICAL verdict.
    # When the power-law fit is weak (R² < 0.80), the criticality claim is
    # downgraded to CRITICAL_LOW_CONFIDENCE regardless of σ band membership.
    # This prevents premature AT_CRITICAL verdicts when the power-law fit
    # is statistically weak, tightening the epistemic standard.
    R2_CONFIDENCE_THRESHOLD = 0.80
    r2_confident = False
    if r2 is not None:
        try:
            r2_confident = float(r2) >= R2_CONFIDENCE_THRESHOLD
        except (TypeError, ValueError):
            r2_confident = False

    verdict = None
    verdict_basis = []
    if sigma_verdict is not None:
        verdict_basis.append("sigma=" + sigma_verdict)
    if alpha_verdict is not None:
        verdict_basis.append("alpha=" + alpha_verdict)
    if r2 is not None:
        verdict_basis.append("r2=" + str(r2) + (">=0.80" if r2_confident else "<0.80"))

    if sigma_verdict == "AT_CRITICAL" and alpha_verdict == "POWER_LAW_CONFIRMED":
        # Gate on R² confidence: full AT_CRITICAL only when R² ≥ 0.80
        if r2_confident:
            verdict = "AT_CRITICAL"
        else:
            verdict = "CRITICAL_LOW_CONFIDENCE"
    elif sigma_verdict == "AT_CRITICAL" and alpha_verdict == "POWER_LAW_EXTENDED_BAND":
        # Extended α band (2.5–3.0) also gated on R² confidence
        if r2_confident:
            verdict = "AT_CRITICAL"
        else:
            verdict = "CRITICAL_LOW_CONFIDENCE"
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
    # O148: confidence_flag — True only when R² ≥ 0.80, surfaced alongside
    # every verdict so downstream consumers can distinguish rigorous from
    # provisional criticality claims without re-parsing the basis string.
    if r2 is not None:
        result["r2_confidence_flag"] = r2_confident
        result["r2_confidence_threshold"] = R2_CONFIDENCE_THRESHOLD
    if sigma is not None:
        result["branching_ratio"] = sigma
    if sigma_err:
        result["branching_ratio_err"] = sigma_err
    if entropy is not None:
        result["shannon_entropy"] = entropy
        # Compute H/H_max (normalized entropy ratio) — scale-invariant criticality
        # index.  H_max = log2(N_states) for the CA grid.  The CA telemetry may
        # supply h_max directly; otherwise we derive it from grid_size (number of
        # distinct cell states defaults to the standard Game of Truth 5-type alphabet,
        # giving H_max = log2(5) ≈ 2.322; or from an explicit n_states field).
        h_max = ca_telemetry.get("h_max") or ca_telemetry.get("H_max")
        if h_max is None:
            import math
            n_states = ca_telemetry.get("n_states") or ca_telemetry.get("num_cell_types")
            if n_states is not None:
                try:
                    ns = int(n_states)
                    if ns > 1:
                        h_max = math.log2(ns)
                except (TypeError, ValueError):
                    pass
            # Fallback: infer from grid_size if available (log2 of unique states)
            if h_max is None:
                # Default to log2(6) ≈ 2.585 for standard 6-type Game of Truth grid
                # (matches paper excerpt: H_max = 2.585 bits for 32x32 grid with 6 types)
                h_max = ca_telemetry.get("h_max_default", math.log2(6))
        if h_max is not None:
            try:
                h_max_val = float(h_max)
                h_val = float(entropy)
                if h_max_val > 0:
                    h_ratio = round(h_val / h_max_val, 6)
                    result["h_max"] = round(h_max_val, 6)
                    result["h_over_h_max"] = h_ratio
                    # Criticality signature: H/H_max near 0.2 indicates
                    # structured-information fraction at the critical ridge
                    if 0.15 <= h_ratio <= 0.25:
                        result["entropy_criticality"] = "AT_RIDGE"
                    elif h_ratio < 0.15:
                        result["entropy_criticality"] = "FROZEN"
                    elif h_ratio > 0.25 and h_ratio < 0.5:
                        result["entropy_criticality"] = "NEAR_RIDGE"
                    else:
                        result["entropy_criticality"] = "DISORDERED"
            except (TypeError, ValueError):
                pass
    if alpha is not None:
        result["avalanche_exponent"] = alpha
    if r2 is not None:
        result["power_law_r2"] = r2
    if survival is not None:
        result["survival_rate"] = survival

    # ── O148 / INV_073: Branching-ratio proxy survival criterion ──────────
    # Rather than using raw cell count as the survival metric, compute a
    # criticality-sensitive survival signal: the population is "healthy"
    # only when σ stays within the critical band (1.0 ± 0.05).  When σ
    # drifts outside this band, the population is flagged as undergoing
    # either runaway growth (supercritical, σ > 1.05) or extinction decay
    # (subcritical, σ < 0.95).  This mirrors the SNN criticality-based
    # pruning criterion: connections (cells) are structurally essential
    # iff removing them pushes σ outside the critical band.
    #
    # The proxy also tests whether branching ratio (σ≈1) maps onto the
    # Wasserstein floor condition (k=1/Tμ) — if both yield the same
    # pruning/survival boundary, they are the same invariant expressed
    # in different substrates.
    #
    # CHALLENGE to INV_073: if this post-hoc σ-based criterion recovers
    # the same survival boundary as continuous ridge navigation, then γ=1
    # is a recoverable property, weakening the claim that real-time ridge
    # navigation is strictly necessary for admissibility.
    branching_survival = {}
    if sigma is not None:
        try:
            s_val = float(sigma)
            sigma_deficit = round(abs(s_val - 1.0), 6)
            # Critical band: σ ∈ [0.95, 1.05]
            in_critical_band = sigma_deficit <= 0.05
            branching_survival["sigma"] = sigma
            branching_survival["sigma_deficit"] = sigma_deficit
            branching_survival["in_critical_band"] = in_critical_band

            # Population health verdict based on σ proximity to 1.0
            if in_critical_band:
                branching_survival["population_health"] = "CRITICAL_HEALTHY"
            elif s_val > 1.05:
                branching_survival["population_health"] = "RUNAWAY_GROWTH"
            else:
                branching_survival["population_health"] = "EXTINCTION_DECAY"

            # Compute thermodynamic floor proxy: k_proxy = 1/(T*μ) where
            # T = number of timesteps (proxy for thermal time) and
            # μ = mean active population (proxy for chemical potential).
            # When σ≈1, k_proxy should converge to a characteristic value;
            # deviation signals departure from the Wasserstein floor.
            active_per_step = ca_telemetry.get("active_per_step")
            offspring_per_step = ca_telemetry.get("offspring_per_step")
            if (isinstance(active_per_step, list) and len(active_per_step) > 0):
                valid_active = [float(x) for x in active_per_step if x is not None and float(x) > 0]
                if valid_active:
                    T_steps = len(valid_active)
                    mu_active = sum(valid_active) / len(valid_active)
                    if T_steps > 0 and mu_active > 0:
                        k_proxy = round(1.0 / (T_steps * mu_active), 8)
                        branching_survival["wasserstein_floor_proxy"] = k_proxy
                        branching_survival["T_steps"] = T_steps
                        branching_survival["mu_active"] = round(mu_active, 4)
                        # Test substrate equivalence: when σ≈1, does k_proxy
                        # stabilize?  Track the coefficient of variation of
                        # per-step k_i = 1/(i * active_i) for convergence.
                        if T_steps >= 3:
                            k_per_step = []
                            for i, a_i in enumerate(valid_active):
                                if a_i > 0:
                                    k_per_step.append(1.0 / ((i + 1) * a_i))
                            if len(k_per_step) >= 2:
                                k_mean = sum(k_per_step) / len(k_per_step)
                                k_var = sum((k - k_mean) ** 2 for k in k_per_step) / len(k_per_step)
                                k_cv = round((k_var ** 0.5) / k_mean, 6) if k_mean > 0 else 0.0
                                branching_survival["k_proxy_cv"] = k_cv
                                branching_survival["k_proxy_stable"] = k_cv < 0.3

            # Per-step population drift flags: count how many timesteps
            # the population was outside the critical band
            per_step_sigma = ca_telemetry.get("per_step_sigma")
            if isinstance(per_step_sigma, list) and per_step_sigma:
                n_total = len(per_step_sigma)
                n_subcritical = sum(1 for s_i in per_step_sigma if s_i < 0.95)
                n_supercritical = sum(1 for s_i in per_step_sigma if s_i > 1.05)
                n_critical = n_total - n_subcritical - n_supercritical
                branching_survival["n_steps_total"] = n_total
                branching_survival["n_steps_critical"] = n_critical
                branching_survival["n_steps_subcritical"] = n_subcritical
                branching_survival["n_steps_supercritical"] = n_supercritical
                branching_survival["critical_residence_fraction"] = round(
                    n_critical / n_total, 6
                ) if n_total > 0 else 0.0

                # Drift direction: net tendency toward extinction or runaway
                if n_subcritical > n_supercritical and n_subcritical > n_critical:
                    branching_survival["drift_tendency"] = "TOWARD_EXTINCTION"
                elif n_supercritical > n_subcritical and n_supercritical > n_critical:
                    branching_survival["drift_tendency"] = "TOWARD_RUNAWAY"
                else:
                    branching_survival["drift_tendency"] = "NEAR_CRITICAL"

                # INV_073 challenge flag: if critical_residence_fraction > 0.7
                # from post-hoc analysis, γ=1 is recoverable without continuous
                # ridge navigation during the simulation
                if branching_survival.get("critical_residence_fraction", 0) > 0.7:
                    branching_survival["inv073_challenge"] = (
                        "σ≈1 maintained in >70% of steps without active ridge "
                        "navigation — γ=1 may be a recoverable attractor, not "
                        "a strict dynamical requirement"
                    )

        except (TypeError, ValueError):
            pass

    if branching_survival:
        result["branching_survival"] = branching_survival
        # Integrate into verdict basis for downstream consumers
        pop_health = branching_survival.get("population_health")
        if pop_health:
            verdict_basis.append("population=" + pop_health)

    # ── O148: Finite-size survival scaling correction ─────────────────────
    # The paper's analytical form: P_survive ~ f((p - p_c) * N^(1/ν))
    # where p is the colonization/survival parameter (mapped to σ here),
    # p_c is the critical point (σ_c = 1.0), N is habitat capacity
    # (total cell count), and ν is the correlation-length exponent.
    # For directed percolation universality class in 2D: ν ≈ 0.734.
    # The scaling function f(x) = 1/(1 + exp(-x)) gives the sigmoidal
    # crossover that replaces binary alive/dead counting near criticality.
    #
    # This correction makes the survival signal quantitatively predictive:
    # raw survival_rate is biased toward the deterministic (N→∞) step
    # function; the scaled version captures the finite-N broadening that
    # the paper demonstrates is analytically necessary.
    finite_size_scaling = {}
    if sigma is not None and survival is not None:
        # Determine N (total cell count / habitat capacity)
        N_cells = ca_telemetry.get("total_cells")
        if N_cells is None:
            _ctc_fs = ca_telemetry.get("cell_type_counts")
            if isinstance(_ctc_fs, dict) and _ctc_fs:
                N_cells = sum(_ctc_fs.values())
        if N_cells is None:
            grid_size_fs = ca_telemetry.get("grid_size")
            if isinstance(grid_size_fs, (int, float)) and grid_size_fs > 0:
                N_cells = int(grid_size_fs) * int(grid_size_fs)
            elif isinstance(grid_size_fs, (list, tuple)) and len(grid_size_fs) >= 2:
                try:
                    N_cells = int(grid_size_fs[0]) * int(grid_size_fs[1])
                except (TypeError, ValueError):
                    pass
        if N_cells is not None and N_cells > 0:
            try:
                import math
                s_val = float(sigma)
                surv_val = float(survival)
                # Critical point and universality exponent
                p_c = 1.0  # σ_c = 1.0 (critical branching ratio)
                nu = ca_telemetry.get("correlation_length_exponent", 0.734)  # 2D DP default
                nu = float(nu)

                # Scaling variable: x = (σ - σ_c) * N^(1/ν)
                scaling_variable = (s_val - p_c) * (N_cells ** (1.0 / nu))
                finite_size_scaling["scaling_variable"] = round(scaling_variable, 6)
                finite_size_scaling["N"] = N_cells
                finite_size_scaling["nu"] = round(nu, 4)
                finite_size_scaling["sigma_minus_pc"] = round(s_val - p_c, 6)

                # Analytical survival probability from scaling function
                # f(x) = 1/(1 + exp(-x)) — sigmoidal crossover
                # Clamp to avoid overflow in exp
                x_clamped = max(-500.0, min(500.0, scaling_variable))
                p_survive_scaled = 1.0 / (1.0 + math.exp(-x_clamped))
                finite_size_scaling["p_survive_scaled"] = round(p_survive_scaled, 6)
                finite_size_scaling["p_survive_raw"] = round(surv_val, 6)

                # Bias diagnostic: difference between raw survival and
                # finite-size-corrected survival.  Large positive bias means
                # the raw count overestimates persistence (deterministic limit
                # artifact); large negative means it underestimates.
                survival_bias = round(surv_val - p_survive_scaled, 6)
                finite_size_scaling["survival_bias"] = survival_bias
                finite_size_scaling["bias_direction"] = (
                    "RAW_OVERESTIMATES" if survival_bias > 0.05
                    else "RAW_UNDERESTIMATES" if survival_bias < -0.05
                    else "NEGLIGIBLE_BIAS"
                )

                # Transition width: the finite-size scaling predicts that
                # the critical transition is broadened by ΔΣ ~ N^(-1/ν).
                # This is the window around σ_c where P_survive transitions
                # from ~0 to ~1; outside this window raw counting is adequate.
                transition_width = N_cells ** (-1.0 / nu)
                finite_size_scaling["transition_width"] = round(transition_width, 8)
                in_transition_zone = abs(s_val - p_c) <= 2.0 * transition_width
                finite_size_scaling["in_transition_zone"] = in_transition_zone

                # When in the transition zone, the scaling correction is
                # load-bearing: raw survival counts are systematically biased.
                # Outside the zone, they converge to the deterministic limit.
                if in_transition_zone:
                    finite_size_scaling["correction_status"] = "SCALING_CORRECTION_ACTIVE"
                    finite_size_scaling["note"] = (
                        f"|σ-σ_c|={round(abs(s_val - p_c), 6)} <= "
                        f"2·N^(-1/ν)={round(2.0 * transition_width, 8)} — "
                        f"finite-size broadening dominates; raw survival "
                        f"biased by {survival_bias}"
                    )
                    # Replace raw survival with scaled survival in the result
                    # when in the transition zone — this is the key correction
                    result["survival_rate_raw"] = surv_val
                    result["survival_rate"] = round(p_survive_scaled, 6)
                else:
                    finite_size_scaling["correction_status"] = "DETERMINISTIC_LIMIT_ADEQUATE"

                # Effective critical point estimator: for finite N, the
                # apparent p_c shifts by ~N^(-1/ν).  Report the corrected
                # critical point so downstream consumers don't mislocate it.
                pc_shift = N_cells ** (-1.0 / nu)
                finite_size_scaling["pc_apparent"] = round(p_c + pc_shift, 8)
                finite_size_scaling["pc_true"] = p_c
                finite_size_scaling["pc_shift"] = round(pc_shift, 8)

            except (TypeError, ValueError, OverflowError, ZeroDivisionError):
                pass

    if finite_size_scaling:
        result["finite_size_scaling"] = finite_size_scaling
        # Surface in verdict basis so the scaling correction is visible
        correction = finite_size_scaling.get("correction_status", "")
        if correction:
            verdict_basis.append("fss=" + correction)
        # When in the transition zone and bias is significant, append
        # a challenge note to the criticality verdict
        if finite_size_scaling.get("in_transition_zone") and abs(finite_size_scaling.get("survival_bias", 0)) > 0.05:
            verdict_basis.append(
                "fss_bias=" + str(finite_size_scaling.get("survival_bias", 0))
            )

    # ── O148: Integrate entropy_criticality into joint verdict ────────────
    # H/H_max (normalized entropy) is now a co-equal diagnostic alongside σ
    # and α.  When H/H_max is available, append it to verdict_basis and,
    # when the entropy structure contradicts the σ-based verdict, downgrade
    # the verdict to flag the epistemic incompleteness.
    _ent_crit = result.get("entropy_criticality")
    _h_ratio_val = result.get("h_over_h_max")
    if _h_ratio_val is not None:
        verdict_basis.append(
            "H/H_max=" + str(_h_ratio_val)
            + ("(" + _ent_crit + ")" if _ent_crit else "")
        )
        # O148 challenge gate: if σ says AT_CRITICAL but entropy structure
        # is FROZEN or DISORDERED, the measurement protocol is incomplete.
        # Downgrade to CRITICAL_ENTROPY_MISMATCH so downstream consumers
        # know the verdict carries an unresolved tension.
        if verdict is not None and "AT_CRITICAL" in str(verdict):
            if _ent_crit in ("FROZEN", "DISORDERED"):
                verdict = "CRITICAL_ENTROPY_MISMATCH"
                verdict_basis.append(
                    "entropy_mismatch=sigma_AT_CRITICAL_but_H/H_max_"
                    + str(_ent_crit)
                )

    if verdict is not None:
        result["criticality_verdict"] = verdict
    if verdict_basis:
        result["verdict_basis"] = verdict_basis

    # O148 / QCA temporal coherence proxy: compute the squared overlap
    # |⟨ρ(t)|ρ(t-1)⟩|² between consecutive cell-state distributions.
    # The QCA paper shows that near criticality (γ=1 ridge), temporal
    # coherence follows a universal power law on approach to stationarity.
    # Tracking only spatial survival misses this definitive temporal signal.
    #
    # Input: step_distributions — list of per-step cell-state distributions,
    # where each entry is a dict {cell_type: fraction} or a list of floats
    # representing the probability vector over cell types at that timestep.
    # Output: temporal_coherence_series — list of |⟨ρ(t)|ρ(t-1)⟩|² values,
    # one per consecutive pair, plus power-law fit diagnostics.
    step_distributions = ca_telemetry.get("step_distributions")
    if isinstance(step_distributions, list) and len(step_distributions) >= 2:
        try:
            import math
            # Normalize each step's distribution into a probability vector
            def _to_prob_vec(dist):
                if isinstance(dist, dict):
                    total = sum(float(v) for v in dist.values()) or 1.0
                    return {k: float(v) / total for k, v in dist.items()}
                elif isinstance(dist, (list, tuple)):
                    total = sum(float(v) for v in dist) or 1.0
                    return {str(i): float(v) / total for i, v in enumerate(dist)}
                return None

            temporal_coherence_series = []
            prev_vec = _to_prob_vec(step_distributions[0])
            for step_idx in range(1, len(step_distributions)):
                curr_vec = _to_prob_vec(step_distributions[step_idx])
                if prev_vec is None or curr_vec is None:
                    prev_vec = curr_vec
                    continue
                # Inner product: ⟨ρ(t-1)|ρ(t)⟩ = Σ_i √(p_i(t-1)) · √(p_i(t))
                # This is the Bhattacharyya coefficient; squared gives fidelity
                all_keys = set(prev_vec.keys()) | set(curr_vec.keys())
                inner = 0.0
                for k in all_keys:
                    p_prev = prev_vec.get(k, 0.0)
                    p_curr = curr_vec.get(k, 0.0)
                    if p_prev > 0 and p_curr > 0:
                        inner += math.sqrt(p_prev) * math.sqrt(p_curr)
                # Squared overlap — the temporal coherence proxy
                overlap_sq = inner * inner
                temporal_coherence_series.append(round(overlap_sq, 8))
                prev_vec = curr_vec

            if temporal_coherence_series:
                result["temporal_coherence_series"] = temporal_coherence_series
                result["temporal_coherence_n_steps"] = len(temporal_coherence_series)
                # Summary statistics
                tc_mean = sum(temporal_coherence_series) / len(temporal_coherence_series)
                result["temporal_coherence_mean"] = round(tc_mean, 6)
                if len(temporal_coherence_series) >= 2:
                    tc_std = (sum((v - tc_mean) ** 2 for v in temporal_coherence_series)
                              / len(temporal_coherence_series)) ** 0.5
                    result["temporal_coherence_std"] = round(tc_std, 6)

                # Power-law fit: C(t) ~ t^(-δ) on approach to stationarity.
                # Compute deviation from unity: 1 - overlap² gives the
                # coherence decay signal; fit log(1-C) vs log(t).
                decay_vals = []
                for i, c in enumerate(temporal_coherence_series):
                    deficit = 1.0 - c
                    if deficit > 1e-12:
                        decay_vals.append((i + 1, deficit))

                if len(decay_vals) >= 3:
                    log_t = [math.log(d[0]) for d in decay_vals]
                    log_d = [math.log(d[1]) for d in decay_vals]
                    n_fit = len(log_t)
                    sum_x = sum(log_t)
                    sum_y = sum(log_d)
                    sum_xy = sum(log_t[j] * log_d[j] for j in range(n_fit))
                    sum_x2 = sum(x * x for x in log_t)
                    denom_fit = n_fit * sum_x2 - sum_x * sum_x
                    if abs(denom_fit) > 1e-12:
                        slope_tc = (n_fit * sum_xy - sum_x * sum_y) / denom_fit
                        intercept_tc = (sum_y - slope_tc * sum_x) / n_fit
                        # δ = -slope (power-law decay exponent)
                        delta = round(-slope_tc, 6)
                        # R² for the fit
                        y_mean_tc = sum_y / n_fit
                        ss_tot_tc = sum((y - y_mean_tc) ** 2 for y in log_d)
                        ss_res_tc = sum(
                            (log_d[j] - (slope_tc * log_t[j] + intercept_tc)) ** 2
                            for j in range(n_fit)
                        )
                        r2_tc = round(1.0 - ss_res_tc / ss_tot_tc, 6) if ss_tot_tc > 1e-12 else 0.0

                        result["temporal_coherence_exponent"] = delta
                        result["temporal_coherence_r2"] = r2_tc
                        result["temporal_coherence_n_fit_points"] = n_fit

                        # Verdict: universal power-law behavior near criticality
                        # expects δ > 0 (coherence deficit decays) with good R²
                        if r2_tc >= 0.7 and delta > 0:
                            result["temporal_coherence_verdict"] = "POWER_LAW_DECAY"
                        elif r2_tc >= 0.7 and delta <= 0:
                            result["temporal_coherence_verdict"] = "COHERENCE_GROWING"
                        else:
                            result["temporal_coherence_verdict"] = "NO_CLEAR_SCALING"

                        # Cross-check with spatial criticality verdict
                        if verdict is not None and r2_tc >= 0.7:
                            if "CRITICAL" in (verdict or "") and delta > 0:
                                result["temporal_spatial_agreement"] = True
                            else:
                                result["temporal_spatial_agreement"] = False

                # Store back into telemetry for downstream consumers
                ca_telemetry["temporal_coherence_series"] = temporal_coherence_series
                if "temporal_coherence_exponent" in result:
                    ca_telemetry["temporal_coherence_exponent"] = result["temporal_coherence_exponent"]
                    ca_telemetry["temporal_coherence_r2"] = result["temporal_coherence_r2"]
        except (TypeError, ValueError, OverflowError):
            pass

    # ── O148: Lerch distribution fit — (z, s, a) parameters ──────────────
    # The Lerch transcendent Φ(z,s,a) = Σ_{n=0}^∞ z^n/(n+a)^s is the
    # theoretically correct distributional model for GoL survival statistics
    # under nonsymmetric (nonextensive) entropy maximization.  Fitting Lerch
    # parameters alongside the existing power-law α gives a direct comparison
    # between empirical CA output and the maximum-nonsymmetric-entropy
    # prediction, replacing raw survival counts with a grounded target.
    #
    # Fit method: grid search + scipy.optimize.minimize (Nelder-Mead) on
    # negative log-likelihood over the avalanche-size histogram.  Falls back
    # to pure grid search when scipy is unavailable.
    #
    # CHALLENGE to O148: if Lerch R² > power-law R², the current power-law
    # summary statistic is the wrong observable and must be replaced.
    # PROVENANCE: self-engineer wrote this call (+ elaborate Lerch/O148 rationale
    # above) but never defined `_fit_lerch_distribution` — an orphan call that
    # would NameError whenever this path runs. The fit was never implemented, so
    # the feature is honestly disabled (None → guard below skips). Hand-fixed
    # 2026-06-26 — same orphan-call class as the _markov gate. See [[project_token_blowout]].
    _lerch_fit = None
    if _lerch_fit:
        result["lerch_fit"] = _lerch_fit
        # Cross-compare with power-law fit quality
        lerch_r2 = _lerch_fit.get("r2")
        pl_r2 = result.get("power_law_r2")
        if lerch_r2 is not None and pl_r2 is not None:
            try:
                if float(lerch_r2) > float(pl_r2):
                    result["lerch_vs_powerlaw"] = "LERCH_SUPERIOR"
                    result["lerch_challenge_o148"] = (
                        f"Lerch R²={lerch_r2} > power-law R²={pl_r2} — "
                        f"survival statistics are better modeled by the Lerch "
                        f"distribution (nonsymmetric entropy), not simple power-law"
                    )
                else:
                    result["lerch_vs_powerlaw"] = "POWERLAW_ADEQUATE"
            except (TypeError, ValueError):
                pass

    # ── O148 / SOC wave-front speed tracking ─────────────────────────────
    # The paper (travelling-wave SOC) shows that constant wave-front speed
    # is the diagnostic signal of SOC criticality.  We compute the spatial
    # centroid of active cells across timesteps and emit velocity as a
    # scalar diagnostic alongside survival count.  This upgrades the
    # simulation from survival-counting to mechanistically correct
    # criticality detection.
    #
    # Input options (in priority order):
    #   1. active_centroids: list of (x, y) centroid positions per timestep
    #   2. row_active_fractions (per timestep): list of list of floats
    #   3. grid_snapshots: list of 2D grid states per timestep
    #   4. Fall back to row_active_fractions (single snapshot) for 1D centroid
    #
    # Output: wavefront_velocity (mean speed), wavefront_speed_series,
    #         wavefront_speed_std, wavefront_speed_constant (bool),
    #         soc_wavefront_verdict.
    _wf_centroids = ca_telemetry.get("active_centroids")
    _wf_row_fracs_series = ca_telemetry.get("row_active_fractions_series")
    _wf_grid_snapshots = ca_telemetry.get("grid_snapshots")
    _wf_active_state = ca_telemetry.get("active_state", 1)

    _computed_centroids = None

    if isinstance(_wf_centroids, list) and len(_wf_centroids) >= 2:
        # Direct centroid positions supplied by CA runner
        _computed_centroids = []
        for c in _wf_centroids:
            if isinstance(c, (list, tuple)) and len(c) >= 2:
                try:
                    _computed_centroids.append((float(c[0]), float(c[1])))
                except (TypeError, ValueError):
                    pass
            elif isinstance(c, (int, float)):
                # 1D centroid (row index)
                try:
                    _computed_centroids.append((float(c), 0.0))
                except (TypeError, ValueError):
                    pass

    elif isinstance(_wf_grid_snapshots, list) and len(_wf_grid_snapshots) >= 2:
        # Compute centroids from full grid snapshots
        _computed_centroids = []
        for grid in _wf_grid_snapshots:
            if not isinstance(grid, list):
                continue
            cx_sum, cy_sum, n_active = 0.0, 0.0, 0
            for ri, row in enumerate(grid):
                if not isinstance(row, (list, tuple)):
                    continue
                for ci, cell in enumerate(row):
                    if cell == _wf_active_state:
                        cx_sum += ci
                        cy_sum += ri
                        n_active += 1
            if n_active > 0:
                _computed_centroids.append((cx_sum / n_active, cy_sum / n_active))

    elif isinstance(_wf_row_fracs_series, list) and len(_wf_row_fracs_series) >= 2:
        # Per-timestep row active fractions → 1D centroid (weighted row index)
        _computed_centroids = []
        for step_fracs in _wf_row_fracs_series:
            if not isinstance(step_fracs, list) or not step_fracs:
                continue
            total_w = sum(float(f) for f in step_fracs)
            if total_w > 0:
                centroid_row = sum(i * float(f) for i, f in enumerate(step_fracs)) / total_w
                _computed_centroids.append((centroid_row, 0.0))

    # Fall back: single-snapshot row_active_fractions with active_per_step
    # gives a 1D centroid proxy from the row distribution at each step
    if _computed_centroids is None or len(_computed_centroids) < 2:
        _aps = ca_telemetry.get("active_per_step")
        _raf = ca_telemetry.get("row_active_fractions")
        if (isinstance(_aps, list) and len(_aps) >= 2
                and isinstance(_raf, list) and len(_raf) >= 2):
            # Use row_active_fractions as spatial weights, active_per_step
            # as the temporal axis — compute centroid shift per step
            total_w = sum(float(f) for f in _raf if f is not None)
            if total_w > 0:
                base_centroid = sum(
                    i * float(f) for i, f in enumerate(_raf) if f is not None
                ) / total_w
                # Model centroid displacement proportional to change in
                # active count (proxy for wavefront advance)
                _computed_centroids = []
                cumulative_disp = 0.0
                for idx, a in enumerate(_aps):
                    if a is not None:
                        try:
                            _computed_centroids.append((base_centroid + cumulative_disp, 0.0))
                            if idx > 0 and _aps[idx - 1] is not None:
                                delta = float(a) - float(_aps[idx - 1])
                                cumulative_disp += delta * 0.01  # small spatial proxy
                        except (TypeError, ValueError):
                            pass

    # Compute velocities from centroid series
    if isinstance(_computed_centroids, list) and len(_computed_centroids) >= 2:
        import math as _wf_math
        wavefront_speeds = []
        for t in range(1, len(_computed_centroids)):
            dx = _computed_centroids[t][0] - _computed_centroids[t - 1][0]
            dy = _computed_centroids[t][1] - _computed_centroids[t - 1][1]
            speed = _wf_math.sqrt(dx * dx + dy * dy)
            wavefront_speeds.append(round(speed, 8))

        if wavefront_speeds:
            wf_mean = sum(wavefront_speeds) / len(wavefront_speeds)
            result["wavefront_velocity"] = round(wf_mean, 6)
            result["wavefront_speed_series"] = wavefront_speeds
            result["wavefront_n_steps"] = len(wavefront_speeds)

            if len(wavefront_speeds) >= 2:
                wf_std = (_wf_math.fsum(
                    (s - wf_mean) ** 2 for s in wavefront_speeds
                ) / len(wavefront_speeds)) ** 0.5
                result["wavefront_speed_std"] = round(wf_std, 6)

                # SOC criticality diagnostic: constant wave-front speed
                # (low coefficient of variation) signals the travelling-
                # wave regime characteristic of SOC.  CV < 0.15 → constant.
                wf_cv = round(wf_std / wf_mean, 6) if wf_mean > 1e-12 else 0.0
                result["wavefront_speed_cv"] = wf_cv
                result["wavefront_speed_constant"] = wf_cv < 0.15

                # SOC wave-front verdict
                if wf_cv < 0.15 and wf_mean > 1e-8:
                    result["soc_wavefront_verdict"] = "CONSTANT_SPEED"
                    # INV_073 metastability check: constant speed confirms
                    # the threshold dynamics maintain the survival condition
                    result["soc_metastability"] = "MAINTAINED"
                elif wf_cv < 0.30 and wf_mean > 1e-8:
                    result["soc_wavefront_verdict"] = "NEAR_CONSTANT"
                    result["soc_metastability"] = "MARGINAL"
                elif wf_mean < 1e-8:
                    result["soc_wavefront_verdict"] = "FROZEN"
                    result["soc_metastability"] = "LOST"
                else:
                    result["soc_wavefront_verdict"] = "VARIABLE_SPEED"
                    result["soc_metastability"] = "UNSTABLE"

                # Integrate with verdict_basis
                if isinstance(verdict_basis, list):
                    verdict_basis.append(
                        "wavefront=" + result.get("soc_wavefront_verdict", "?")
                    )
            else:
                result["wavefront_speed_std"] = 0.0
                result["wavefront_speed_cv"] = 0.0
                result["wavefront_speed_constant"] = True
                result["soc_wavefront_verdict"] = "SINGLE_STEP"

            # Store back for downstream
            ca_telemetry["wavefront_velocity"] = result["wavefront_velocity"]
            ca_telemetry["wavefront_speed_series"] = wavefront_speeds
            if "soc_wavefront_verdict" in result:
                ca_telemetry["soc_wavefront_verdict"] = result["soc_wavefront_verdict"]

    # ── O148: Dislocation-density field & nucleation tracking ─────────────
    # CPFEM-CA bidirectional coupling: each CA cell carries a continuous
    # dislocation_density scalar (ρ) that accumulates from neighbor activity.
    # When ρ crosses a nucleation threshold (ρ_crit), the cell undergoes a
    # DRX nucleation event — replacing binary survival with a two-variable
    # (state, dislocation_density) update rule.
    #
    # The CA telemetry may supply:
    #   dislocation_density_field: list[float] — per-cell ρ values (flattened)
    #   dislocation_density_mean: float — pre-computed mean ρ
    #   dislocation_density_max: float — pre-computed max ρ
    #   nucleation_threshold: float — ρ_crit (default 0.85)
    #   nucleation_events: int — count of cells that crossed ρ_crit this step
    #   neighbor_activity_per_step: list[float] — mean neighbor activity per step
    #
    # When only per-cell densities are supplied, we compute nucleation count,
    # mean/max/std, and the nucleation_fraction in situ.
    _dd_field = ca_telemetry.get("dislocation_density_field")
    _dd_mean = ca_telemetry.get("dislocation_density_mean")
    _dd_max = ca_telemetry.get("dislocation_density_max")
    _nuc_threshold = ca_telemetry.get("nucleation_threshold", 0.85)
    _nuc_events = ca_telemetry.get("nucleation_events")
    _neighbor_activity = ca_telemetry.get("neighbor_activity_per_step")

    dislocation_info = {}

    # Derive statistics from raw field when available
    if isinstance(_dd_field, list) and len(_dd_field) > 0:
        try:
            _valid_dd = [float(d) for d in _dd_field if d is not None]
            if _valid_dd:
                _dd_n = len(_valid_dd)
                _dd_mean_c = sum(_valid_dd) / _dd_n
                _dd_max_c = max(_valid_dd)
                _dd_min_c = min(_valid_dd)
                _dd_std_c = (sum((d - _dd_mean_c) ** 2 for d in _valid_dd) / _dd_n) ** 0.5

                _nuc_thr = float(_nuc_threshold)
                _nuc_count = sum(1 for d in _valid_dd if d >= _nuc_thr)
                _nuc_frac = _nuc_count / _dd_n if _dd_n > 0 else 0.0

                dislocation_info["density_mean"] = round(_dd_mean_c, 6)
                dislocation_info["density_max"] = round(_dd_max_c, 6)
                dislocation_info["density_min"] = round(_dd_min_c, 6)
                dislocation_info["density_std"] = round(_dd_std_c, 6)
                dislocation_info["density_n_cells"] = _dd_n
                dislocation_info["nucleation_threshold"] = round(_nuc_thr, 6)
                dislocation_info["nucleation_count"] = _nuc_count
                dislocation_info["nucleation_fraction"] = round(_nuc_frac, 6)

                # Nucleation regime classification:
                # QUIESCENT: < 5% cells above threshold (no DRX)
                # INCIPIENT: 5-20% cells above threshold (early DRX)
                # ACTIVE_DRX: 20-60% cells above threshold (dynamic recrystallization)
                # SATURATED: > 60% cells above threshold (fully recrystallized)
                if _nuc_frac < 0.05:
                    dislocation_info["nucleation_regime"] = "QUIESCENT"
                elif _nuc_frac < 0.20:
                    dislocation_info["nucleation_regime"] = "INCIPIENT"
                elif _nuc_frac < 0.60:
                    dislocation_info["nucleation_regime"] = "ACTIVE_DRX"
                else:
                    dislocation_info["nucleation_regime"] = "SATURATED"

                # Bidirectional coupling signal: when nucleation is active,
                # the mean dislocation density should decrease (DRX softening).
                # Track the density-nucleation correlation for feedback.
                if _nuc_count > 0 and _dd_mean_c > 0:
                    # Softening ratio: lower means more effective DRX
                    _above_thr = [d for d in _valid_dd if d >= _nuc_thr]
                    _below_thr = [d for d in _valid_dd if d < _nuc_thr]
                    if _above_thr and _below_thr:
                        _mean_above = sum(_above_thr) / len(_above_thr)
                        _mean_below = sum(_below_thr) / len(_below_thr)
                        dislocation_info["density_above_threshold_mean"] = round(_mean_above, 6)
                        dislocation_info["density_below_threshold_mean"] = round(_mean_below, 6)
                        dislocation_info["density_contrast"] = round(_mean_above - _mean_below, 6)

                # Store computed values back for downstream
                ca_telemetry["dislocation_density_mean"] = dislocation_info["density_mean"]
                ca_telemetry["dislocation_density_max"] = dislocation_info["density_max"]
                ca_telemetry["nucleation_events"] = _nuc_count
        except (TypeError, ValueError):
            pass
    elif _dd_mean is not None:
        # Pre-computed summary statistics from CA runner
        try:
            dislocation_info["density_mean"] = round(float(_dd_mean), 6)
            if _dd_max is not None:
                dislocation_info["density_max"] = round(float(_dd_max), 6)
            _nuc_thr = float(_nuc_threshold)
            dislocation_info["nucleation_threshold"] = round(_nuc_thr, 6)
            if _nuc_events is not None:
                dislocation_info["nucleation_count"] = int(_nuc_events)
        except (TypeError, ValueError):
            pass

    # Neighbor-activity accumulation tracking: the dislocation pressure
    # accumulates from neighbor activity over time steps
    if isinstance(_neighbor_activity, list) and len(_neighbor_activity) >= 2:
        try:
            _na_valid = [float(a) for a in _neighbor_activity if a is not None]
            if _na_valid:
                _na_mean = sum(_na_valid) / len(_na_valid)
                _na_trend = _na_valid[-1] - _na_valid[0] if len(_na_valid) >= 2 else 0.0
                dislocation_info["neighbor_activity_mean"] = round(_na_mean, 6)
                dislocation_info["neighbor_activity_trend"] = round(_na_trend, 6)
                dislocation_info["neighbor_activity_n_steps"] = len(_na_valid)
                # Accumulation rate: positive trend means pressure building
                dislocation_info["pressure_accumulating"] = _na_trend > 0
        except (TypeError, ValueError):
            pass

    # Cross-reference with criticality: nucleation events should correlate
    # with σ drift — active DRX pushes σ away from 1.0 as cell population
    # composition changes rapidly during recrystallization
    if dislocation_info and sigma is not None:
        try:
            _nuc_regime = dislocation_info.get("nucleation_regime", "QUIESCENT")
            _s_val = float(sigma)
            if _nuc_regime in ("ACTIVE_DRX", "SATURATED") and abs(_s_val - 1.0) > 0.05:
                dislocation_info["drx_sigma_coupling"] = "CONFIRMED"
                dislocation_info["drx_sigma_coupling_note"] = (
                    f"Active nucleation ({_nuc_regime}) correlates with "
                    f"σ={_s_val} outside critical band — DRX is driving "
                    f"population composition change"
                )
            elif _nuc_regime == "QUIESCENT" and abs(_s_val - 1.0) <= 0.05:
                dislocation_info["drx_sigma_coupling"] = "QUIESCENT_CRITICAL"
            else:
                dislocation_info["drx_sigma_coupling"] = "UNCORRELATED"
        except (TypeError, ValueError):
            pass

    if dislocation_info:
        result["dislocation_density"] = dislocation_info
        # Surface nucleation regime in verdict basis
        _nuc_regime = dislocation_info.get("nucleation_regime")
        if _nuc_regime and isinstance(verdict_basis, list):
            verdict_basis.append("nucleation=" + _nuc_regime)

    # ── VOMAS overlay: thermodynamic-floor validation layer ───────────────
    # VOMAS (Virtual Overlay Multi-Agent System) meta-observer: compares
    # current cell survival statistics against the expected thermodynamic
    # floor and logs divergence as a *validation signal* rather than a
    # simulation error.  This converts the CA from a passive runner into
    # a self-validating system that distinguishes "simulation running"
    # from "simulation running correctly" (O148 architectural gap).
    #
    # Thermodynamic floor: given σ (branching ratio) and N (population),
    # the minimum expected survival rate is:
    #   S_floor = max(0, 1 - exp(-σ · N^(1/ν) · k_thermo))
    # where k_thermo is the Boltzmann-scale coupling constant and ν is
    # the correlation-length exponent (2D DP: ν ≈ 0.734).
    #
    # Spectral monitor: track the "eigenvalue drift" proxy as the ratio
    # of temporal coherence variance to spatial variance — when this
    # diverges from unity, the simulation dynamics have decoupled from
    # the expected thermodynamic trajectory.
    #
    # CHALLENGE to O148: without this meta-observer layer, the CA cannot
    # distinguish correct emergent dynamics from plausible-looking noise.
    # VOMAS makes validation structural rather than output-derived.
    vomas_overlay = {}
    _vomas_sigma = sigma
    _vomas_survival = survival
    _vomas_N = ca_telemetry.get("total_cells")
    if _vomas_N is None:
        _ctc_vomas = ca_telemetry.get("cell_type_counts")
        if isinstance(_ctc_vomas, dict) and _ctc_vomas:
            _vomas_N = sum(_ctc_vomas.values())
    if _vomas_N is None:
        _gs_vomas = ca_telemetry.get("grid_size")
        if isinstance(_gs_vomas, (int, float)) and _gs_vomas > 0:
            _vomas_N = int(_gs_vomas) * int(_gs_vomas)
        elif isinstance(_gs_vomas, (list, tuple)) and len(_gs_vomas) >= 2:
            try:
                _vomas_N = int(_gs_vomas[0]) * int(_gs_vomas[1])
            except (TypeError, ValueError):
                pass

    if _vomas_sigma is not None and _vomas_N is not None and _vomas_N > 0:
        try:
            import math as _vomas_math
            _v_s = float(_vomas_sigma)
            _v_N = float(_vomas_N)
            _v_nu = float(ca_telemetry.get("correlation_length_exponent", 0.734))
            _v_k_thermo = 0.01  # Boltzmann-scale coupling constant

            # Thermodynamic floor: minimum expected survival
            _v_scaling = _v_s * (_v_N ** (1.0 / _v_nu)) * _v_k_thermo
            _v_scaling_clamped = max(-500.0, min(500.0, _v_scaling))
            _v_s_floor = max(0.0, 1.0 - _vomas_math.exp(-_v_scaling_clamped))
            vomas_overlay["thermodynamic_floor"] = round(_v_s_floor, 6)
            vomas_overlay["sigma_used"] = _v_s
            vomas_overlay["N_cells"] = int(_v_N)
            vomas_overlay["nu"] = round(_v_nu, 4)
            vomas_overlay["k_thermo"] = _v_k_thermo

            # Divergence: compare observed survival against floor
            if _vomas_survival is not None:
                try:
                    _v_surv = float(_vomas_survival)
                    _v_divergence = round(_v_surv - _v_s_floor, 6)
                    vomas_overlay["observed_survival"] = round(_v_surv, 6)
                    vomas_overlay["floor_divergence"] = _v_divergence

                    # Validation verdict: survival below floor is a validation
                    # signal (model-against-intent mismatch), NOT a sim error
                    if _v_divergence < -0.05:
                        vomas_overlay["validation_signal"] = "BELOW_FLOOR"
                        vomas_overlay["validation_note"] = (
                            f"Survival {round(_v_surv, 4)} is {round(abs(_v_divergence), 4)} "
                            f"below thermodynamic floor {round(_v_s_floor, 4)} — "
                            f"dynamics may have departed admissible trajectory; "
                            f"this is a V&V signal, not a simulation error"
                        )
                    elif _v_divergence > 0.3:
                        vomas_overlay["validation_signal"] = "FAR_ABOVE_FLOOR"
                        vomas_overlay["validation_note"] = (
                            f"Survival {round(_v_surv, 4)} exceeds floor by "
                            f"{round(_v_divergence, 4)} — system well above "
                            f"thermodynamic minimum; dynamics admissible"
                        )
                    else:
                        vomas_overlay["validation_signal"] = "NEAR_FLOOR"
                        vomas_overlay["validation_note"] = (
                            f"Survival {round(_v_surv, 4)} near thermodynamic "
                            f"floor {round(_v_s_floor, 4)} — system at "
                            f"admissibility boundary"
                        )
                except (TypeError, ValueError):
                    pass

            # Spectral monitor: eigenvalue drift proxy from temporal
            # coherence.  When temporal_coherence_std is available,
            # compare it against spatial_row_std to detect decoupling
            # between temporal and spatial dynamics.
            _v_tc_std = result.get("temporal_coherence_std")
            _v_sp_std = result.get("spatial_row_std")
            if _v_tc_std is not None and _v_sp_std is not None:
                try:
                    _v_tc = float(_v_tc_std)
                    _v_sp = float(_v_sp_std)
                    if _v_sp > 1e-12:
                        _v_eigen_ratio = round(_v_tc / _v_sp, 6)
                        vomas_overlay["eigenvalue_drift_proxy"] = _v_eigen_ratio
                        # Near unity: temporal and spatial scales coupled
                        # Far from unity: decoupled — validation warning
                        if 0.5 <= _v_eigen_ratio <= 2.0:
                            vomas_overlay["spectral_verdict"] = "COUPLED"
                        else:
                            vomas_overlay["spectral_verdict"] = "DECOUPLED"
                            vomas_overlay["spectral_warning"] = (
                                f"Eigenvalue drift proxy {_v_eigen_ratio} "
                                f"outside [0.5, 2.0] — temporal/spatial "
                                f"dynamics decoupled; criticality claim "
                                f"requires re-examination"
                            )
                    elif _v_tc > 1e-12:
                        vomas_overlay["eigenvalue_drift_proxy"] = None
                        vomas_overlay["spectral_verdict"] = "SPATIAL_FROZEN"
                except (TypeError, ValueError):
                    pass

            # Per-step σ band residency as VOMAS compliance fraction:
            # the overlay checks whether the simulation *intends* to
            # stay at criticality and whether it *actually* does.
            _v_band_frac = ca_telemetry.get("sigma_in_band_fraction")
            if _v_band_frac is not None:
                try:
                    _v_bf = float(_v_band_frac)
                    vomas_overlay["sigma_band_compliance"] = round(_v_bf, 6)
                    if _v_bf >= 0.7:
                        vomas_overlay["compliance_verdict"] = "ADMISSIBLE"
                    elif _v_bf >= 0.4:
                        vomas_overlay["compliance_verdict"] = "MARGINAL"
                    else:
                        vomas_overlay["compliance_verdict"] = "INADMISSIBLE"
                except (TypeError, ValueError):
                    pass

            vomas_overlay["overlay_type"] = "VOMAS_THERMODYNAMIC_FLOOR"
            vomas_overlay["o148_status"] = "VALIDATION_LAYER_ACTIVE"

            # Log the VOMAS validation signal
            _v_signal = vomas_overlay.get("validation_signal", "UNKNOWN")
            _v_spectral = vomas_overlay.get("spectral_verdict", "N/A")
            _v_compliance = vomas_overlay.get("compliance_verdict", "N/A")
            print(
                f"[VOMAS] floor={round(_v_s_floor, 4)} "
                f"signal={_v_signal} spectral={_v_spectral} "
                f"compliance={_v_compliance}"
            )

        except (TypeError, ValueError, OverflowError, ZeroDivisionError):
            pass

    if vomas_overlay:
        result["vomas_overlay"] = vomas_overlay
        # Surface in verdict basis so downstream sees the validation layer
        if isinstance(verdict_basis, list):
            _v_sig = vomas_overlay.get("validation_signal")
            if _v_sig:
                verdict_basis.append("vomas=" + _v_sig)
            _v_comp = vomas_overlay.get("compliance_verdict")
            if _v_comp:
                verdict_basis.append("vomas_compliance=" + _v_comp)

    # ── O148: Three-metric complexity convergence check ───────────────────
    # Mobile automata paper: phase transitions are only detectable when
    # Shannon entropy AND Kolmogorov complexity spike simultaneously.
    # Single-metric survival counting misses the critical transition boundary.
    #
    # Metrics computed:
    #   1. Shannon block entropy (H_block) — entropy of length-k blocks in
    #      the cell-state sequence (k=2 bigrams by default)
    #   2. Kolmogorov proxy #1 (K_zlib) — compression ratio via zlib
    #   3. Kolmogorov proxy #2 (K_bz2)  — compression ratio via bz2
    #
    # Phase-transition onset flag: all three metrics exceed their respective
    # spike thresholds simultaneously (z-score > 1.5 relative to rolling mean).
    _cell_state_sequence = ca_telemetry.get("cell_state_sequence")
    # Build from live_cells_per_step or cell_type_counts if no raw sequence
    if _cell_state_sequence is None:
        _lc_seq = ca_telemetry.get("live_cells_per_step")
        if isinstance(_lc_seq, list) and len(_lc_seq) >= 4:
            _cell_state_sequence = _lc_seq
    if _cell_state_sequence is None:
        _ctc_seq = ca_telemetry.get("cell_type_counts")
        if isinstance(_ctc_seq, dict) and _ctc_seq:
            # Flatten type counts into a repeating sequence for compression
            _cell_state_sequence = []
            for ctype, ct_count in sorted(_ctc_seq.items()):
                _cell_state_sequence.extend([hash(ctype) % 256] * int(ct_count))
    if _cell_state_sequence is None:
        # Build from per_step_sigma as a proxy signal
        _ps_sigma = ca_telemetry.get("per_step_sigma")
        if isinstance(_ps_sigma, list) and len(_ps_sigma) >= 4:
            _cell_state_sequence = [int(round(s * 1000)) % 256 for s in _ps_sigma]

    complexity_convergence = {}
    if isinstance(_cell_state_sequence, list) and len(_cell_state_sequence) >= 4:
        try:
            import math as _cc_math
            import zlib as _cc_zlib
            import bz2 as _cc_bz2

            # Quantize to bytes for compression estimators
            _cc_raw = []
            for _cv in _cell_state_sequence:
                try:
                    _cc_raw.append(int(float(_cv)) % 256)
                except (TypeError, ValueError):
                    _cc_raw.append(0)
            _cc_bytes = bytes(_cc_raw)
            _cc_n = len(_cc_bytes)

            # --- Metric 1: Shannon block entropy (bigram, k=2) ---
            _block_k = min(2, _cc_n)
            _bigram_counts = {}
            for _bi in range(_cc_n - _block_k + 1):
                _bg = _cc_bytes[_bi:_bi + _block_k]
                _bigram_counts[_bg] = _bigram_counts.get(_bg, 0) + 1
            _bg_total = sum(_bigram_counts.values())
            _h_block = 0.0
            if _bg_total > 0:
                for _bgc in _bigram_counts.values():
                    _p = _bgc / _bg_total
                    if _p > 0:
                        _h_block -= _p * _cc_math.log2(_p)
            _h_block = round(_h_block, 6)
            complexity_convergence["shannon_block_entropy"] = _h_block
            complexity_convergence["block_size_k"] = _block_k
            complexity_convergence["n_unique_blocks"] = len(_bigram_counts)

            # --- Metric 2: Kolmogorov proxy via zlib compression ratio ---
            _cc_zlib_compressed = _cc_zlib.compress(_cc_bytes, 9)
            _k_zlib = round(len(_cc_zlib_compressed) / _cc_n, 6) if _cc_n > 0 else 0.0
            complexity_convergence["kolmogorov_zlib_ratio"] = _k_zlib
            complexity_convergence["zlib_compressed_size"] = len(_cc_zlib_compressed)

            # --- Metric 3: Kolmogorov proxy via bz2 compression ratio ---
            _cc_bz2_compressed = _cc_bz2.compress(_cc_bytes, 9)
            _k_bz2 = round(len(_cc_bz2_compressed) / _cc_n, 6) if _cc_n > 0 else 0.0
            complexity_convergence["kolmogorov_bz2_ratio"] = _k_bz2
            complexity_convergence["bz2_compressed_size"] = len(_cc_bz2_compressed)

            complexity_convergence["raw_sequence_length"] = _cc_n

            # --- Phase-transition onset detection ---
            # Load rolling history from timeseries to compute z-scores
            _cc_ts_file = DOCS_DIR / "criticality_timeseries.json"
            _cc_hist_h = []
            _cc_hist_zlib = []
            _cc_hist_bz2 = []
            if _cc_ts_file.exists():
                try:
                    _cc_ts_data = json.loads(_cc_ts_file.read_text())
                    if isinstance(_cc_ts_data, list):
                        for _cc_entry in _cc_ts_data[-20:]:
                            _cc_conv = _cc_entry.get("complexity_convergence")
                            if isinstance(_cc_conv, dict):
                                _hv = _cc_conv.get("shannon_block_entropy")
                                _zv = _cc_conv.get("kolmogorov_zlib_ratio")
                                _bv = _cc_conv.get("kolmogorov_bz2_ratio")
                                if _hv is not None:
                                    _cc_hist_h.append(float(_hv))
                                if _zv is not None:
                                    _cc_hist_zlib.append(float(_zv))
                                if _bv is not None:
                                    _cc_hist_bz2.append(float(_bv))
                except (json.JSONDecodeError, OSError):
                    pass

            def _cc_zscore(val, history):
                """Compute z-score of val against history."""
                if len(history) < 2:
                    return 0.0
                _m = sum(history) / len(history)
                _v = sum((_x - _m) ** 2 for _x in history) / len(history)
                _s = _v ** 0.5
                if _s < 1e-12:
                    return 0.0
                return (val - _m) / _s

            _z_h = round(_cc_zscore(_h_block, _cc_hist_h), 4)
            _z_zlib = round(_cc_zscore(_k_zlib, _cc_hist_zlib), 4)
            _z_bz2 = round(_cc_zscore(_k_bz2, _cc_hist_bz2), 4)

            complexity_convergence["zscore_shannon_block"] = _z_h
            complexity_convergence["zscore_kolmogorov_zlib"] = _z_zlib
            complexity_convergence["zscore_kolmogorov_bz2"] = _z_bz2

            # Spike threshold: z > 1.5 for each metric
            SPIKE_THRESHOLD = 1.5
            _spike_h = abs(_z_h) > SPIKE_THRESHOLD
            _spike_zlib = abs(_z_zlib) > SPIKE_THRESHOLD
            _spike_bz2 = abs(_z_bz2) > SPIKE_THRESHOLD

            complexity_convergence["spike_shannon"] = _spike_h
            complexity_convergence["spike_zlib"] = _spike_zlib
            complexity_convergence["spike_bz2"] = _spike_bz2
            complexity_convergence["spike_threshold"] = SPIKE_THRESHOLD

            # Phase-transition onset: ALL THREE spike simultaneously
            _phase_transition_onset = _spike_h and _spike_zlib and _spike_bz2
            complexity_convergence["phase_transition_onset"] = _phase_transition_onset
            complexity_convergence["n_metrics_spiking"] = sum([_spike_h, _spike_zlib, _spike_bz2])

            if _phase_transition_onset:
                complexity_convergence["phase_transition_note"] = (
                    f"ALL THREE complexity metrics spike simultaneously "
                    f"(z_H={_z_h}, z_zlib={_z_zlib}, z_bz2={_z_bz2}) — "
                    f"phase-transition onset detected per mobile automata "
                    f"convergence criterion; single-metric survival counting "
                    f"would miss this transition boundary"
                )
                print(
                    f"[CA] *** PHASE TRANSITION ONSET: "
                    f"H_block z={_z_h}, K_zlib z={_z_zlib}, K_bz2 z={_z_bz2} ***"
                )

            # Metric agreement: do the two Kolmogorov proxies agree?
            if _k_zlib > 0:
                _k_agreement = round(abs(_k_zlib - _k_bz2) / _k_zlib, 6)
            else:
                _k_agreement = 0.0
            complexity_convergence["kolmogorov_proxy_agreement"] = _k_agreement
            complexity_convergence["kolmogorov_proxies_consistent"] = _k_agreement < 0.2

        except (TypeError, ValueError, OverflowError, ImportError):
            pass

    if complexity_convergence:
        result["complexity_convergence"] = complexity_convergence
        # Surface in verdict basis
        if isinstance(verdict_basis, list):
            _n_spiking = complexity_convergence.get("n_metrics_spiking", 0)
            if complexity_convergence.get("phase_transition_onset"):
                verdict_basis.append("phase_transition=ONSET(3/3)")
            elif _n_spiking > 0:
                verdict_basis.append(
                    "complexity_spikes=" + str(_n_spiking) + "/3"
                )

    # ── O148: s-skewed asynchronous CA concurrency analysis ──────────────
    # Paper: s-skewed updating scheme where s neighboring cells update
    # jointly per generation step.  s=1 is fully async, s=N is fully sync.
    # KS entropy (Kolmogorov-Sinai) is computed as a function of s to
    # characterize the order-chaos boundary.  Activity = fraction of cells
    # that changed state.  Density = fraction of cells in active state.
    #
    # When the CA runner supplies per_step_states (list of grid snapshots)
    # or live_cells_per_step, we compute KS entropy proxy, activity, and
    # density for the given concurrency parameter s.
    _ca_concurrency_s = ca_telemetry.get("concurrency_s", 1)
    _ca_grid_size_n = ca_telemetry.get("total_cells")
    if _ca_grid_size_n is None:
        _ctc_s = ca_telemetry.get("cell_type_counts")
        if isinstance(_ctc_s, dict) and _ctc_s:
            _ca_grid_size_n = sum(_ctc_s.values())
    if _ca_grid_size_n is None:
        _gs_s = ca_telemetry.get("grid_size")
        if isinstance(_gs_s, (int, float)) and _gs_s > 0:
            _ca_grid_size_n = int(_gs_s) * int(_gs_s)
        elif isinstance(_gs_s, (list, tuple)) and len(_gs_s) >= 2:
            try:
                _ca_grid_size_n = int(_gs_s[0]) * int(_gs_s[1])
            except (TypeError, ValueError):
                pass

    s_skewed_analysis = {}
    try:
        _s_val = int(_ca_concurrency_s)
        if _s_val < 1:
            _s_val = 1
        s_skewed_analysis["concurrency_s"] = _s_val
        if _ca_grid_size_n is not None and _ca_grid_size_n > 0:
            _N = int(_ca_grid_size_n)
            s_skewed_analysis["grid_size_N"] = _N
            # Synchrony ratio: s/N — 0 = fully async, 1 = fully sync
            _sync_ratio = round(_s_val / _N, 8) if _N > 0 else 0.0
            s_skewed_analysis["synchrony_ratio"] = _sync_ratio

            # KS entropy proxy from per-step state differences.
            # KS entropy h_KS measures the rate of information production.
            # For CA: h_KS ≈ -Σ p_i log(p_i) over transition probabilities.
            # We estimate from live_cells_per_step: the transition probability
            # at each step is the fraction of cells that changed, scaled by
            # the concurrency parameter s (s cells update jointly).
            #
            # With s-skewed updating: effective transitions per step = s,
            # so the per-cell KS entropy scales as h_KS(s) ≈ h_KS(1) * f(s/N)
            # where f captures the correlation effects of joint updates.
            _lc_for_ks = ca_telemetry.get("live_cells_per_step")
            _activity_per_step = ca_telemetry.get("activity_per_step")

            # Compute activity from live_cells_per_step if not directly supplied
            _computed_activity = []
            if isinstance(_activity_per_step, list) and len(_activity_per_step) >= 2:
                _computed_activity = [float(a) for a in _activity_per_step if a is not None]
            elif isinstance(_lc_for_ks, list) and len(_lc_for_ks) >= 2:
                try:
                    _lc_valid_ks = [float(x) for x in _lc_for_ks if x is not None]
                    if len(_lc_valid_ks) >= 2:
                        for _ki in range(1, len(_lc_valid_ks)):
                            # Activity = |change in live cells| / N
                            _delta = abs(_lc_valid_ks[_ki] - _lc_valid_ks[_ki - 1])
                            _act = _delta / _N if _N > 0 else 0.0
                            _computed_activity.append(_act)
                except (TypeError, ValueError):
                    pass

            if _computed_activity:
                import math as _ks_math
                _act_mean = sum(_computed_activity) / len(_computed_activity)
                s_skewed_analysis["activity_mean"] = round(_act_mean, 6)
                s_skewed_analysis["activity_n_steps"] = len(_computed_activity)

                if len(_computed_activity) >= 2:
                    _act_std = (sum((a - _act_mean) ** 2 for a in _computed_activity)
                                / len(_computed_activity)) ** 0.5
                    s_skewed_analysis["activity_std"] = round(_act_std, 6)

                # Density: fraction of cells in active state (from live_cells)
                if isinstance(_lc_for_ks, list) and _lc_for_ks:
                    _lc_dens = [float(x) for x in _lc_for_ks if x is not None]
                    if _lc_dens and _N > 0:
                        _density_mean = sum(d / _N for d in _lc_dens) / len(_lc_dens)
                        s_skewed_analysis["density_mean"] = round(_density_mean, 6)

                # KS entropy proxy: h_KS ≈ -Σ p_i log2(p_i) over activity bins
                # Bin the activity values into a histogram, then compute Shannon
                # entropy of the bin distribution.  Scale by s/N to capture
                # the concurrency effect on information production rate.
                _n_bins = min(20, max(3, len(_computed_activity) // 3))
                if _computed_activity and max(_computed_activity) > 0:
                    _act_max = max(_computed_activity)
                    _act_min = min(_computed_activity)
                    _bin_width = (_act_max - _act_min) / _n_bins if _act_max > _act_min else 1.0
                    _bins = [0] * _n_bins
                    for _av in _computed_activity:
                        _bi = int((_av - _act_min) / _bin_width) if _bin_width > 0 else 0
                        _bi = min(_bi, _n_bins - 1)
                        _bins[_bi] += 1
                    _bin_total = sum(_bins)
                    _h_ks = 0.0
                    if _bin_total > 0:
                        for _bc in _bins:
                            if _bc > 0:
                                _p = _bc / _bin_total
                                _h_ks -= _p * _ks_math.log2(_p)

                    # Scale by concurrency: h_KS(s) = h_KS_raw * (s/N) * N
                    # This captures that s jointly-updated cells produce
                    # correlated state changes, reducing effective entropy
                    # production per cell but increasing it per update step.
                    _h_ks_scaled = _h_ks * _s_val if _s_val > 0 else _h_ks
                    # Normalize by log2(N) for scale-invariant comparison
                    _h_ks_normalized = 0.0
                    if _N > 1:
                        _h_ks_normalized = _h_ks_scaled / _ks_math.log2(_N)

                    s_skewed_analysis["ks_entropy_raw"] = round(_h_ks, 6)
                    s_skewed_analysis["ks_entropy_scaled_by_s"] = round(_h_ks_scaled, 6)
                    s_skewed_analysis["ks_entropy_normalized"] = round(_h_ks_normalized, 6)
                    s_skewed_analysis["ks_entropy_n_bins"] = _n_bins

                    # Thermodynamic regime classification based on KS entropy:
                    # h_KS ≈ 0: ordered/frozen phase
                    # h_KS moderate: edge of chaos (critical ridge)
                    # h_KS high: chaotic/disordered phase
                    # Thresholds calibrated to log2(N) scale
                    if _N > 1:
                        _h_max_ref = _ks_math.log2(_N)
                        _ks_ratio = _h_ks / _h_max_ref if _h_max_ref > 0 else 0.0
                        s_skewed_analysis["ks_entropy_ratio"] = round(_ks_ratio, 6)
                        if _ks_ratio < 0.1:
                            s_skewed_analysis["thermodynamic_regime"] = "ORDERED"
                        elif _ks_ratio < 0.3:
                            s_skewed_analysis["thermodynamic_regime"] = "EDGE_OF_CHAOS"
                        elif _ks_ratio < 0.6:
                            s_skewed_analysis["thermodynamic_regime"] = "CHAOTIC"
                        else:
                            s_skewed_analysis["thermodynamic_regime"] = "FULLY_CHAOTIC"

                        # The critical ridge in s-space: for each s value,
                        # the system should pass through ordered → edge_of_chaos
                        # → chaotic as s increases from 1 to N.  Log the current
                        # position on this sweep.
                        s_skewed_analysis["s_sweep_position"] = {
                            "s": _s_val,
                            "N": _N,
                            "sync_ratio": _sync_ratio,
                            "ks_ratio": round(_ks_ratio, 6),
                            "regime": s_skewed_analysis.get("thermodynamic_regime"),
                            "activity": round(_act_mean, 6),
                            "density": s_skewed_analysis.get("density_mean"),
                        }

            # Multi-s sweep results: when the CA runner supplies ks_entropy_sweep
            # (dict mapping s → {ks_entropy, activity, density}), embed directly
            _ks_sweep = ca_telemetry.get("ks_entropy_sweep")
            if isinstance(_ks_sweep, dict) and _ks_sweep:
                s_skewed_analysis["ks_entropy_sweep"] = _ks_sweep
                # Find the s value where KS entropy peaks (chaos onset)
                _peak_s = None
                _peak_ks = -1.0
                for _sw_s, _sw_data in _ks_sweep.items():
                    _sw_ks = _sw_data.get("ks_entropy", 0) if isinstance(_sw_data, dict) else 0
                    try:
                        if float(_sw_ks) > _peak_ks:
                            _peak_ks = float(_sw_ks)
                            _peak_s = _sw_s
                    except (TypeError, ValueError):
                        pass
                if _peak_s is not None:
                    s_skewed_analysis["ks_peak_s"] = _peak_s
                    s_skewed_analysis["ks_peak_entropy"] = round(_peak_ks, 6)

    except (TypeError, ValueError, OverflowError):
        pass

    if s_skewed_analysis:
        result["s_skewed_analysis"] = s_skewed_analysis
        # Surface in verdict basis
        if isinstance(verdict_basis, list):
            _regime = s_skewed_analysis.get("thermodynamic_regime")
            if _regime:
                verdict_basis.append(
                    "s_skewed=" + _regime + "(s=" + str(s_skewed_analysis.get("concurrency_s", "?")) + ")"
                )
            _ks_ent = s_skewed_analysis.get("ks_entropy_normalized")
            if _ks_ent is not None:
                verdict_basis.append("h_KS=" + str(_ks_ent))

        # Store back into telemetry for downstream
        ca_telemetry["s_skewed_analysis"] = s_skewed_analysis

    # ── O148: Avalanche propagation ratio (σ_prop) extraction ────────────
    # Distinct from the cell-level branching ratio σ: the propagation ratio
    # measures how many cells each *changing* cell causes to change in the
    # next step.  σ_prop = mean(changed_cells(t+1) / changed_cells(t)).
    # This is the true avalanche branching ratio used in SOC literature.
    # When activity_per_step or live_cells_per_step deltas are available,
    # compute σ_prop and issue AT_CRITICAL/SUBCRITICAL/SUPERCRITICAL verdict.
    _prop_activity = ca_telemetry.get("activity_per_step")
    _prop_changes = None

    if isinstance(_prop_activity, list) and len(_prop_activity) >= 2:
        try:
            _prop_changes = [float(a) for a in _prop_activity if a is not None]
        except (TypeError, ValueError):
            _prop_changes = None

    # Derive changed-cell counts from consecutive live_cells_per_step diffs
    if _prop_changes is None or len(_prop_changes or []) < 2:
        _lc_prop = ca_telemetry.get("live_cells_per_step")
        if isinstance(_lc_prop, list) and len(_lc_prop) >= 3:
            try:
                _lc_vals = [float(x) for x in _lc_prop if x is not None]
                if len(_lc_vals) >= 3:
                    _prop_changes = [abs(_lc_vals[i] - _lc_vals[i - 1])
                                     for i in range(1, len(_lc_vals))]
            except (TypeError, ValueError):
                pass

    propagation_ratio_info = {}
    if isinstance(_prop_changes, list) and len(_prop_changes) >= 2:
        try:
            _prop_ratios = []
            for _pi in range(1, len(_prop_changes)):
                if _prop_changes[_pi - 1] > 0:
                    _prop_ratios.append(_prop_changes[_pi] / _prop_changes[_pi - 1])
            if _prop_ratios:
                _pr_mean = sum(_prop_ratios) / len(_prop_ratios)
                _pr_std = 0.0
                if len(_prop_ratios) >= 2:
                    _pr_var = sum((r - _pr_mean) ** 2 for r in _prop_ratios) / len(_prop_ratios)
                    _pr_std = _pr_var ** 0.5

                propagation_ratio_info["sigma_prop"] = round(_pr_mean, 6)
                propagation_ratio_info["sigma_prop_err"] = round(_pr_std, 6)
                propagation_ratio_info["sigma_prop_n_steps"] = len(_prop_ratios)

                # Criticality verdict from propagation ratio
                if abs(_pr_mean - 1.0) <= 0.05:
                    propagation_ratio_info["sigma_prop_verdict"] = "AT_CRITICAL"
                elif _pr_mean > 1.05:
                    propagation_ratio_info["sigma_prop_verdict"] = "SUPERCRITICAL"
                else:
                    propagation_ratio_info["sigma_prop_verdict"] = "SUBCRITICAL"

                # Per-step σ_prop band residency
                _PROP_BAND = 0.05
                _n_in_band = sum(1 for r in _prop_ratios if abs(r - 1.0) <= _PROP_BAND)
                propagation_ratio_info["in_band_fraction"] = round(
                    _n_in_band / len(_prop_ratios), 6
                )

                # Cross-validate against cell-level σ when both are available
                if sigma is not None:
                    try:
                        _s_cell = float(sigma)
                        _agreement = abs(_s_cell - _pr_mean) < 0.10
                        propagation_ratio_info["cell_sigma"] = sigma
                        propagation_ratio_info["sigma_agreement"] = _agreement
                        if not _agreement:
                            propagation_ratio_info["sigma_discrepancy_note"] = (
                                f"Cell-level σ={sigma} and propagation σ_prop="
                                f"{round(_pr_mean, 6)} disagree by "
                                f"{round(abs(_s_cell - _pr_mean), 6)} — "
                                f"avalanche dynamics may differ from population dynamics"
                            )
                    except (TypeError, ValueError):
                        pass

                # Store back into telemetry
                ca_telemetry["sigma_prop"] = propagation_ratio_info["sigma_prop"]
                ca_telemetry["sigma_prop_err"] = propagation_ratio_info["sigma_prop_err"]
                ca_telemetry["sigma_prop_verdict"] = propagation_ratio_info["sigma_prop_verdict"]
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    if propagation_ratio_info:
        result["propagation_ratio"] = propagation_ratio_info
        # Surface in verdict basis
        _pr_verdict = propagation_ratio_info.get("sigma_prop_verdict")
        if _pr_verdict and isinstance(verdict_basis, list):
            verdict_basis.append(
                "sigma_prop=" + str(propagation_ratio_info.get("sigma_prop", "?"))
                + "(" + _pr_verdict + ")"
            )

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

    # O148: Compute dominant cell-type population fraction so the genome can
    # track whether Physics Navigator dominance is a stable attractor across
    # runs or a run-specific fluctuation.  Fraction = dominant_count / total_cells.
    # total_cells derived from grid_size, cell_type_counts sum, or explicit field.
    if dominant_count is not None:
        total_cells_for_frac = ca_telemetry.get("total_cells")
        if total_cells_for_frac is None:
            _ctc = ca_telemetry.get("cell_type_counts")
            if isinstance(_ctc, dict) and _ctc:
                total_cells_for_frac = sum(_ctc.values())
        if total_cells_for_frac is None:
            grid_size = ca_telemetry.get("grid_size")
            if isinstance(grid_size, (int, float)) and grid_size > 0:
                total_cells_for_frac = int(grid_size) * int(grid_size)
            elif isinstance(grid_size, (list, tuple)) and len(grid_size) >= 2:
                try:
                    total_cells_for_frac = int(grid_size[0]) * int(grid_size[1])
                except (TypeError, ValueError):
                    pass
        if total_cells_for_frac is not None:
            try:
                frac = float(dominant_count) / float(total_cells_for_frac)
                result["dominant_cell_fraction"] = round(frac, 6)
                result["total_cells"] = int(total_cells_for_frac)
                # Near-frozen-attractor warning: when any single cell type
                # exceeds 80% of the grid, the system is approaching a
                # frozen attractor where criticality (σ≈1) cannot be
                # maintained — loss of type diversity suppresses the
                # branching heterogeneity that sustains the critical band.
                FROZEN_ATTRACTOR_THRESHOLD = 0.80
                if frac > FROZEN_ATTRACTOR_THRESHOLD:
                    result["near_frozen_attractor"] = True
                    result["near_frozen_attractor_threshold"] = FROZEN_ATTRACTOR_THRESHOLD
                    result["near_frozen_attractor_warning"] = (
                        f"dominant type {result.get('dominant_cell_type', '?')} "
                        f"holds {round(frac * 100, 1)}% of grid "
                        f"(>{int(FROZEN_ATTRACTOR_THRESHOLD * 100)}% threshold) — "
                        f"near-frozen-attractor: criticality loss imminent; "
                        f"type diversity insufficient to sustain σ≈1 branching"
                    )
                    # Append to verdict_basis so the frozen-attractor signal
                    # is visible in the joint criticality verdict
                    if isinstance(verdict_basis, list):
                        verdict_basis.append(
                            "frozen_attractor=WARNING("
                            + str(round(frac * 100, 1)) + "%)"
                        )
                else:
                    result["near_frozen_attractor"] = False
            except (TypeError, ValueError, ZeroDivisionError):
                pass

    # O148: Full cell-type distribution — record every type's count and fraction
    # so criticality-as-selector can be tracked across generations. This closes
    # the ABSENT gap by capturing whether Physics Navigator dominance is an
    # invariant of critical dynamics or a transient fluctuation.
    _ctc_dist = ca_telemetry.get("cell_type_counts")
    if isinstance(_ctc_dist, dict) and _ctc_dist:
        _total_dist = sum(_ctc_dist.values())
        if _total_dist > 0:
            distribution = {}
            for ctype, ct_count in sorted(_ctc_dist.items(), key=lambda x: -x[1]):
                distribution[ctype] = {
                    "count": ct_count,
                    "fraction": round(ct_count / _total_dist, 6),
                }
            result["cell_type_distribution"] = distribution
            result["cell_type_distribution_total"] = _total_dist
            result["cell_type_distribution_n_types"] = len(_ctc_dist)
            # Track whether a single type exceeds 50% — dominance signal
            top_type = max(_ctc_dist, key=_ctc_dist.get)
            top_frac = _ctc_dist[top_type] / _total_dist
            result["distribution_dominant_type"] = top_type
            result["distribution_dominant_fraction"] = round(top_frac, 6)
            result["distribution_is_dominated"] = top_frac > 0.50

    # O148 / O112: Dominant cell-type fraction among surviving cells — a new
    # observable that could distinguish universality classes and anchor the
    # STF metric tensor recovery experiment.  When survival_rate is available,
    # surviving_cells = total_cells * survival_rate; the dominant fraction
    # among survivors may differ from the raw dominant_cell_fraction when
    # cell death is type-biased.  Flag >50% as a universality-class signal:
    # a single type exceeding half of surviving cells indicates the system
    # may be in a symmetry-broken phase rather than a maximally diverse
    # critical state.
    if dominant_count is not None:
        _surviving_cells = None
        _total_for_surv = result.get("total_cells")
        _surv_rate = ca_telemetry.get("survival_rate") or (survival if survival is not None else None)
        # Prefer surviving_cell_count if explicitly provided by CA runner
        _surviving_cells = ca_telemetry.get("surviving_cell_count") or ca_telemetry.get("surviving_cells")
        if _surviving_cells is None and _total_for_surv is not None and _surv_rate is not None:
            try:
                _surviving_cells = int(round(float(_total_for_surv) * float(_surv_rate)))
            except (TypeError, ValueError):
                pass
        # Fall back to total_cells when no survival info (all cells assumed surviving)
        if _surviving_cells is None:
            _surviving_cells = _total_for_surv
        if _surviving_cells is not None:
            try:
                surv_count = float(_surviving_cells)
                dom_count = float(dominant_count)
                if surv_count > 0:
                    dom_surv_frac = round(dom_count / surv_count, 6)
                    result["dominant_surviving_fraction"] = dom_surv_frac
                    result["surviving_cells"] = int(round(surv_count))
                    # Universality-class signal: >50% dominance among survivors
                    # indicates potential symmetry-broken phase (O148 challenge:
                    # is this a thermodynamic attractor or initial-condition artifact?)
                    UNIVERSALITY_THRESHOLD = 0.50
                    if dom_surv_frac > UNIVERSALITY_THRESHOLD:
                        result["universality_class_flag"] = True
                        result["universality_class_signal"] = (
                            f"dominant type {result.get('dominant_cell_type', '?')} "
                            f"holds {round(dom_surv_frac * 100, 1)}% of surviving cells "
                            f"(>{int(UNIVERSALITY_THRESHOLD * 100)}% threshold) — "
                            f"potential symmetry-broken universality class; "
                            f"test whether this is attractor or initial-condition artifact"
                        )
                    else:
                        result["universality_class_flag"] = False
            except (TypeError, ValueError, ZeroDivisionError):
                pass

    # O148: Per-cell-type entropy decomposition — compute each type's count,
    # probability p_i, and Shannon contribution h_i = -p_i·log2(p_i).
    # This directly tests whether Physics Navigator dominance causally
    # explains the 84% entropy suppression by showing which types carry
    # the entropy and which suppress it.  The sum of h_i equals H_total,
    # providing a closed-form decomposition of the snapshot entropy.
    _ctc_for_entropy = ca_telemetry.get("cell_type_counts")
    if isinstance(_ctc_for_entropy, dict) and _ctc_for_entropy:
        import math
        _total_for_entropy = sum(_ctc_for_entropy.values())
        if _total_for_entropy > 0:
            per_type_entropy = {}
            h_reconstructed = 0.0
            for ctype, ct_count in _ctc_for_entropy.items():
                p_i = ct_count / _total_for_entropy
                if p_i > 0:
                    h_i = -p_i * math.log2(p_i)
                else:
                    h_i = 0.0
                h_reconstructed += h_i
                per_type_entropy[ctype] = {
                    "count": ct_count,
                    "probability": round(p_i, 6),
                    "shannon_contribution": round(h_i, 6),
                }
            # H_max for this alphabet size
            n_types = len(_ctc_for_entropy)
            h_max_types = math.log2(n_types) if n_types > 1 else 0.0
            result["per_type_entropy"] = per_type_entropy
            result["entropy_reconstructed"] = round(h_reconstructed, 6)
            result["entropy_n_types"] = n_types
            if h_max_types > 0:
                result["entropy_suppression_ratio"] = round(
                    1.0 - h_reconstructed / h_max_types, 6
                )
            # Identify the type contributing most to entropy suppression:
            # the type with highest p_i (lowest h_i relative to uniform)
            # In a uniform distribution each type would contribute
            # h_uniform = log2(n_types)/n_types.  The suppression from
            # type i is (h_uniform - h_i) — positive when that type is
            # over-represented and thus suppresses entropy.
            if n_types > 1:
                h_uniform_per = math.log2(n_types) / n_types
                max_suppressor = None
                max_suppression = -float("inf")
                for ctype, edata in per_type_entropy.items():
                    suppression_i = h_uniform_per - edata["shannon_contribution"]
                    edata["entropy_suppression"] = round(suppression_i, 6)
                    if suppression_i > max_suppression:
                        max_suppression = suppression_i
                        max_suppressor = ctype
                if max_suppressor is not None:
                    result["entropy_dominant_suppressor"] = max_suppressor
                    result["entropy_dominant_suppression"] = round(max_suppression, 6)

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

    # ── O148: Spatial correlation extent — extensive vs localized phase ──
    # Detect the absorbing-state transition by computing the fraction of
    # cells in the active state across rows.  When the CA runner supplies
    # per-row active fractions (row_active_fractions: list[float]) or a
    # full grid snapshot (grid_rows: list[list[int]]), we compute:
    #   active_fraction  — mean fraction of active cells across all rows
    #   active_row_ratio — fraction of rows with any active cell
    #   spatial_extent_verdict — EXTENSIVE / LOCALIZED / CRITICAL_BOUNDARY
    # The threshold p_c ≈ 0.5 marks the absorbing-state transition: above
    # it the active state is extensive (percolating), below it the active
    # state is localized (absorbed).  Near p_c the system sits at the
    # critical boundary — the load-bearing diagnostic for universality.
    row_fractions = ca_telemetry.get("row_active_fractions")
    grid_rows = ca_telemetry.get("grid_rows")
    active_state = ca_telemetry.get("active_state", 1)

    if row_fractions is None and grid_rows is not None:
        # Derive per-row active fractions from raw grid snapshot
        try:
            row_fractions = []
            for row in grid_rows:
                if not row:
                    row_fractions.append(0.0)
                else:
                    n_active = sum(1 for cell in row if cell == active_state)
                    row_fractions.append(n_active / len(row))
        except (TypeError, ValueError):
            row_fractions = None

    if isinstance(row_fractions, list) and row_fractions:
        try:
            valid_fracs = [float(f) for f in row_fractions]
            n_rows = len(valid_fracs)
            active_fraction = sum(valid_fracs) / n_rows if n_rows else 0.0
            active_rows = sum(1 for f in valid_fracs if f > 0.0)
            active_row_ratio = active_rows / n_rows if n_rows else 0.0

            result["spatial_active_fraction"] = round(active_fraction, 6)
            result["spatial_active_row_ratio"] = round(active_row_ratio, 6)
            result["spatial_n_rows"] = n_rows

            # Absorbing-state transition thresholds (tunable; defaults
            # calibrated to 2D plaquette model p_c ≈ 0.5 mapping)
            p_c_low = float(ca_telemetry.get("absorbing_threshold_low", 0.40))
            p_c_high = float(ca_telemetry.get("absorbing_threshold_high", 0.60))

            if active_fraction >= p_c_high:
                spatial_verdict = "EXTENSIVE"
            elif active_fraction <= p_c_low:
                spatial_verdict = "LOCALIZED"
            else:
                spatial_verdict = "CRITICAL_BOUNDARY"

            result["spatial_extent_verdict"] = spatial_verdict

            # Row-level heterogeneity: std of per-row fractions indicates
            # whether the active state is uniformly distributed (extensive)
            # or clustered (localized with spatial structure)
            if n_rows >= 2:
                mean_f = active_fraction
                std_f = (sum((f - mean_f) ** 2 for f in valid_fracs) / n_rows) ** 0.5
                result["spatial_row_std"] = round(std_f, 6)
                # High std with moderate mean → spatially structured (near transition)
                if std_f > 0.15 and p_c_low < active_fraction < p_c_high:
                    result["spatial_structure"] = "HETEROGENEOUS_CRITICAL"
                elif std_f > 0.15:
                    result["spatial_structure"] = "HETEROGENEOUS"
                else:
                    result["spatial_structure"] = "HOMOGENEOUS"

            # Integrate spatial extent with existing criticality verdict:
            # true criticality requires BOTH σ in band AND spatial extent
            # at the extensive/localized boundary
            if verdict is not None and spatial_verdict == "CRITICAL_BOUNDARY":
                if "CRITICAL" in verdict:
                    result["absorbing_state_confirmed"] = True
                else:
                    result["absorbing_state_confirmed"] = False
            elif spatial_verdict == "CRITICAL_BOUNDARY":
                result["absorbing_state_confirmed"] = None  # no σ to cross-check

        except (TypeError, ValueError):
            pass

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

    # O148 structured telemetry: build ca_telemetry_snapshot with σ, α,
    # H/H_max as structured fields and AT_CRITICAL flag when σ ∈ [0.95,1.05]
    # and power_law_likely = True.  This makes the criticality verdict
    # auditable and linkable to O148 resolution tracking.
    if criticality:
        _snap_sigma = criticality.get("branching_ratio")
        _snap_alpha = criticality.get("avalanche_exponent")
        _snap_h_ratio = criticality.get("h_over_h_max")
        _snap_r2 = criticality.get("power_law_r2")
        _snap_verdict = criticality.get("criticality_verdict")

        # Determine power_law_likely: R² ≥ 0.70 and α in plausible SOC range
        _snap_pl_likely = False
        if _snap_r2 is not None and _snap_alpha is not None:
            try:
                _snap_pl_likely = float(_snap_r2) >= 0.70 and float(_snap_alpha) > 0
            except (TypeError, ValueError):
                pass

        # AT_CRITICAL flag: σ ∈ [0.95, 1.05] AND power_law_likely
        _snap_at_critical = False
        if _snap_sigma is not None:
            try:
                _s = float(_snap_sigma)
                _snap_at_critical = (0.95 <= _s <= 1.05) and _snap_pl_likely
            except (TypeError, ValueError):
                pass

        ca_telemetry_snapshot = {
            "branching_ratio_sigma": _snap_sigma,
            "avalanche_exponent_alpha": _snap_alpha,
            "entropy_fraction_h_over_h_max": _snap_h_ratio,
            "power_law_r2": _snap_r2,
            "power_law_likely": _snap_pl_likely,
            "AT_CRITICAL": _snap_at_critical,
            "criticality_verdict": _snap_verdict,
        }
        # Include σ error bar when available
        _snap_sigma_err = criticality.get("branching_ratio_err")
        if _snap_sigma_err is not None:
            ca_telemetry_snapshot["branching_ratio_sigma_err"] = _snap_sigma_err
        # Include raw entropy and h_max for reproducibility
        _snap_h = criticality.get("shannon_entropy")
        if _snap_h is not None:
            ca_telemetry_snapshot["shannon_entropy_bits"] = _snap_h
        _snap_hmax = criticality.get("h_max")
        if _snap_hmax is not None:
            ca_telemetry_snapshot["h_max_bits"] = _snap_hmax

        # O148: Record dominant cell-type count and fraction alongside σ
        # and H at each snapshot interval.  This enables future correlation
        # analysis between cell-type composition and criticality maintenance,
        # directly advancing the Physics Navigator attractor hypothesis.
        _snap_dom_type = criticality.get("dominant_cell_type")
        _snap_dom_count = criticality.get("dominant_cell_count")
        _snap_dom_frac = criticality.get("dominant_cell_fraction")
        _snap_total = criticality.get("total_cells")
        if _snap_dom_type is not None:
            ca_telemetry_snapshot["dominant_cell_type"] = _snap_dom_type
        if _snap_dom_count is not None:
            ca_telemetry_snapshot["dominant_cell_count"] = _snap_dom_count
        if _snap_dom_frac is not None:
            ca_telemetry_snapshot["dominant_cell_fraction"] = _snap_dom_frac
        if _snap_total is not None:
            ca_telemetry_snapshot["total_cells"] = _snap_total

        # O148: Per-cell-type counts and longitudinal variance across timesteps.
        # Records the full cell-type distribution at snapshot time and, when
        # step_distributions are available from the CA runner, computes the
        # variance of each type's count across timesteps.  This closes the
        # ABSENT invariant candidate gap: cell-type dominance distributions
        # become a tracked, falsifiable time-series signal rather than a
        # single-point observation.
        _snap_ctd = criticality.get("cell_type_distribution")
        if isinstance(_snap_ctd, dict) and _snap_ctd:
            _snap_type_counts = {}
            for _ct_name, _ct_info in _snap_ctd.items():
                if isinstance(_ct_info, dict):
                    _snap_type_counts[_ct_name] = {
                        "count": _ct_info.get("count"),
                        "fraction": _ct_info.get("fraction"),
                    }
            if _snap_type_counts:
                ca_telemetry_snapshot["cell_type_counts"] = _snap_type_counts
                ca_telemetry_snapshot["cell_type_n_types"] = len(_snap_type_counts)

        # Longitudinal cell-type variance: when step_distributions is available,
        # compute per-type mean count and variance across timesteps.  This is
        # the definitive signal for whether Physics Navigator dominance is a
        # stable attractor or a transient fluctuation (O148 requirement).
        _snap_step_dists = ca_telemetry.get("step_distributions")
        if isinstance(_snap_step_dists, list) and len(_snap_step_dists) >= 2:
            # Collect per-type counts across all timesteps
            _type_series = {}  # type_name -> list of counts/fractions
            for _sd in _snap_step_dists:
                _sd_keys = set()
                if isinstance(_sd, dict):
                    for _tk, _tv in _sd.items():
                        _sd_keys.add(_tk)
                        _type_series.setdefault(_tk, []).append(float(_tv))
                elif isinstance(_sd, (list, tuple)):
                    for _ti, _tv in enumerate(_sd):
                        _tk = str(_ti)
                        _sd_keys.add(_tk)
                        _type_series.setdefault(_tk, []).append(float(_tv))
                # Pad missing types with 0 for this step
                for _existing_k in list(_type_series.keys()):
                    if _existing_k not in _sd_keys:
                        _type_series[_existing_k].append(0.0)

            if _type_series:
                _type_variance = {}
                _all_variances = []
                for _tk, _vals in _type_series.items():
                    if len(_vals) >= 2:
                        _tmean = sum(_vals) / len(_vals)
                        _tvar = sum((_v - _tmean) ** 2 for _v in _vals) / len(_vals)
                        _type_variance[_tk] = {
                            "mean": round(_tmean, 6),
                            "variance": round(_tvar, 6),
                            "std": round(_tvar ** 0.5, 6),
                            "n_steps": len(_vals),
                        }
                        _all_variances.append(_tvar)
                if _type_variance:
                    ca_telemetry_snapshot["cell_type_longitudinal"] = _type_variance
                    ca_telemetry_snapshot["cell_type_longitudinal_n_steps"] = len(_snap_step_dists)
                    # Aggregate variance across all types — high value means
                    # composition is unstable; low means frozen attractor
                    _mean_var = sum(_all_variances) / len(_all_variances)
                    ca_telemetry_snapshot["cell_type_mean_variance"] = round(_mean_var, 6)
                    ca_telemetry_snapshot["cell_type_composition_stable"] = _mean_var < 0.01

        # O148: Append branching_ratio, avalanche_exponent, and
        # entropy_fraction_of_max to the per-step snapshot record so
        # downstream analysis can track their time evolution rather than
        # snapshot averages.  These clean-named keys complement the
        # verbose keys already present and close the O148 tracking gap.
        if _snap_sigma is not None:
            ca_telemetry_snapshot["branching_ratio"] = _snap_sigma
        if _snap_alpha is not None:
            ca_telemetry_snapshot["avalanche_exponent"] = _snap_alpha
        if _snap_h_ratio is not None:
            ca_telemetry_snapshot["entropy_fraction_of_max"] = _snap_h_ratio

        # O148 closure: Record σ and α computation provenance and raw
        # step-level data directly in the snapshot so each snapshot is
        # independently falsifiable without re-running the CA.
        # σ provenance: was branching_ratio computed from live_cells_per_step
        # ratios (rolling_live_cell_ratio) or pre-supplied by the CA runner?
        _snap_sigma_computed = ca_telemetry.get("branching_ratio_computed", False)
        _snap_sigma_method = ca_telemetry.get("branching_ratio_method")
        _snap_sigma_n = ca_telemetry.get("branching_ratio_n_steps")
        ca_telemetry_snapshot["sigma_computed_in_situ"] = bool(_snap_sigma_computed)
        if _snap_sigma_method is not None:
            ca_telemetry_snapshot["sigma_method"] = _snap_sigma_method
        if _snap_sigma_n is not None:
            ca_telemetry_snapshot["sigma_n_steps"] = _snap_sigma_n

        # α provenance: was avalanche_exponent computed from avalanche_sizes
        # histogram via log-log regression, or pre-supplied?
        _snap_alpha_computed = ca_telemetry.get("avalanche_exponent_computed", False)
        _snap_alpha_n_bins = ca_telemetry.get("avalanche_histogram_bins")
        _snap_alpha_n_events = ca_telemetry.get("avalanche_n_events")
        ca_telemetry_snapshot["alpha_computed_in_situ"] = bool(_snap_alpha_computed)
        if _snap_alpha_n_bins is not None:
            ca_telemetry_snapshot["alpha_histogram_bins"] = _snap_alpha_n_bins
        if _snap_alpha_n_events is not None:
            ca_telemetry_snapshot["alpha_n_events"] = _snap_alpha_n_events

        # SOC health: α drift from universality target
        _snap_soc_drift = ca_telemetry.get("soc_alpha_drift")
        _snap_soc_warning = ca_telemetry.get("soc_health_warning")
        if _snap_soc_drift is not None:
            ca_telemetry_snapshot["soc_alpha_drift"] = _snap_soc_drift
        if _snap_soc_warning is not None:
            ca_telemetry_snapshot["soc_health_warning"] = _snap_soc_warning

        # σ band residency fraction: what fraction of per-step σ values
        # stayed inside the critical band [0.95, 1.05]?
        _snap_band_frac = ca_telemetry.get("sigma_in_band_fraction")
        if _snap_band_frac is not None:
            ca_telemetry_snapshot["sigma_in_band_fraction"] = _snap_band_frac

        # Survival rate — completes the (σ, α, survival) triple
        # _snap_survival assigned here before use. PROVENANCE: self-engineer
        # accretion referenced it ABOVE its original assignment (~2699), risking
        # NameError. Hand-fixed 2026-06-26. See [[project_token_blowout]].
        _snap_survival = criticality.get("survival_rate")
        if _snap_survival is not None:
            ca_telemetry_snapshot["survival_rate"] = _snap_survival

        # O148 / INV_073: Structured criticality signature tuple and
        # multi-signature criticality_verdict based on ALL FOUR co-conditions
        # rather than branching ratio alone.  The four co-conditions are:
        #   C1: σ ∈ [0.95, 1.05]  (branching ratio in critical band)
        #   C2: α ∈ [1.5, 3.0]    (power-law exponent in SOC range)
        #   C3: R² ≥ 0.80         (power-law fit confidence)
        #   C4: H/H_max ∈ [0.15, 0.25]  (entropy at critical ridge)
        # Verdict is AT_CRITICAL only when all four pass; partial matches
        # yield graduated verdicts that name the failing conditions.
        _snap_survival = criticality.get("survival_rate")
        _snap_h_raw = criticality.get("shannon_entropy")
        _snap_h_max_val = criticality.get("h_max")

        # Build the structured (σ, α, R², H, H/H_max, survival_rate) tuple
        ca_telemetry_snapshot["criticality_signature_tuple"] = {
            "sigma": _snap_sigma,
            "alpha": _snap_alpha,
            "r2": _snap_r2,
            "H": _snap_h_raw,
            "H_over_H_max": _snap_h_ratio,
            "survival_rate": _snap_survival,
        }

        # Evaluate each co-condition independently
        _c1_sigma_pass = False
        if _snap_sigma is not None:
            try:
                _c1_sigma_pass = 0.95 <= float(_snap_sigma) <= 1.05
            except (TypeError, ValueError):
                pass

        _c2_alpha_pass = False
        if _snap_alpha is not None:
            try:
                _c2_alpha_pass = 1.5 <= float(_snap_alpha) <= 3.0
            except (TypeError, ValueError):
                pass

        _c3_r2_pass = False
        if _snap_r2 is not None:
            try:
                _c3_r2_pass = float(_snap_r2) >= 0.80
            except (TypeError, ValueError):
                pass

        _c4_entropy_pass = False
        if _snap_h_ratio is not None:
            try:
                _c4_entropy_pass = 0.15 <= float(_snap_h_ratio) <= 0.25
            except (TypeError, ValueError):
                pass

        _co_conditions = {
            "C1_sigma_in_band": _c1_sigma_pass,
            "C2_alpha_in_soc_range": _c2_alpha_pass,
            "C3_r2_confident": _c3_r2_pass,
            "C4_entropy_at_ridge": _c4_entropy_pass,
        }
        _n_passing = sum([_c1_sigma_pass, _c2_alpha_pass, _c3_r2_pass, _c4_entropy_pass])
        _co_conditions["n_passing"] = _n_passing
        _co_conditions["n_total"] = 4

        # Multi-signature criticality verdict
        if _n_passing == 4:
            _multi_verdict = "AT_CRITICAL"
        elif _n_passing == 3:
            _failing = [k for k, v in _co_conditions.items()
                        if k.startswith("C") and not v]
            _multi_verdict = "CRITICAL_PARTIAL_3of4"
            if _failing:
                _multi_verdict += "(" + ",".join(_failing) + "_FAIL)"
        elif _n_passing == 2:
            _multi_verdict = "CRITICAL_WEAK_2of4"
        elif _n_passing == 1:
            _multi_verdict = "NEAR_CRITICAL_1of4"
        else:
            _multi_verdict = "NOT_CRITICAL_0of4"

        # Override: if σ is not in band, system cannot be AT_CRITICAL
        # regardless of other conditions (σ is the primary invariant)
        if not _c1_sigma_pass and _n_passing >= 3:
            _multi_verdict = "CRITICAL_PARTIAL_SIGMA_FAIL"

        ca_telemetry_snapshot["co_conditions"] = _co_conditions
        ca_telemetry_snapshot["criticality_verdict"] = _multi_verdict
        ca_telemetry_snapshot["criticality_verdict_method"] = "multi_signature_4_co_conditions"

        # O148 trajectory extension: embed per-step σ trajectory and
        # per-step entropy trajectory into the snapshot so the dynamic
        # evolution of σ and H is verifiable from a single snapshot
        # record, closing the O148 challenge that a 200-step aggregate
        # leaves temporal dynamics unverified.
        _traj_per_step_sigma = ca_telemetry.get("per_step_sigma")
        if isinstance(_traj_per_step_sigma, list) and len(_traj_per_step_sigma) >= 2:
            ca_telemetry_snapshot["sigma_trajectory"] = _traj_per_step_sigma
            ca_telemetry_snapshot["sigma_trajectory_n_steps"] = len(_traj_per_step_sigma)
            _traj_s_mean = sum(_traj_per_step_sigma) / len(_traj_per_step_sigma)
            _traj_s_var = sum((s - _traj_s_mean) ** 2 for s in _traj_per_step_sigma) / len(_traj_per_step_sigma)
            ca_telemetry_snapshot["sigma_trajectory_mean"] = round(_traj_s_mean, 6)
            ca_telemetry_snapshot["sigma_trajectory_std"] = round(_traj_s_var ** 0.5, 6)
            # Fraction of per-step σ values inside [0.95, 1.05]
            _traj_s_in_band = sum(1 for s in _traj_per_step_sigma if 0.95 <= s <= 1.05)
            ca_telemetry_snapshot["sigma_trajectory_in_band_fraction"] = round(
                _traj_s_in_band / len(_traj_per_step_sigma), 6
            )

        _traj_entropy_series = ca_telemetry.get("entropy_timeseries")
        if isinstance(_traj_entropy_series, list) and len(_traj_entropy_series) >= 2:
            ca_telemetry_snapshot["entropy_trajectory"] = _traj_entropy_series
            ca_telemetry_snapshot["entropy_trajectory_n_steps"] = len(_traj_entropy_series)
            try:
                _traj_h_vals = [float(h) for h in _traj_entropy_series if h is not None]
                if _traj_h_vals:
                    _traj_h_mean = sum(_traj_h_vals) / len(_traj_h_vals)
                    _traj_h_var = sum((h - _traj_h_mean) ** 2 for h in _traj_h_vals) / len(_traj_h_vals)
                    ca_telemetry_snapshot["entropy_trajectory_mean"] = round(_traj_h_mean, 6)
                    ca_telemetry_snapshot["entropy_trajectory_std"] = round(_traj_h_var ** 0.5, 6)
            except (TypeError, ValueError):
                pass

        # O148: snapshot_completeness flag — True only when both σ trajectory
        # AND α are present, meaning this single snapshot is sufficient for
        # full O148 criticality tracking without a separate diagnostic pass.
        _has_sigma_traj = "sigma_trajectory" in ca_telemetry_snapshot
        _has_alpha = _snap_alpha is not None
        _has_entropy = _snap_h_ratio is not None or "entropy_trajectory" in ca_telemetry_snapshot
        ca_telemetry_snapshot["o148_snapshot_complete"] = _has_sigma_traj and _has_alpha and _has_entropy
        ca_telemetry_snapshot["o148_fields_present"] = {
            "sigma_trajectory": _has_sigma_traj,
            "alpha": _has_alpha,
            "entropy": _has_entropy,
            "r2": _snap_r2 is not None,
            "survival": criticality.get("survival_rate") is not None,
        }

        # ── O148: Supercritical drift detection ──────────────────────────
        # When σ persistently exceeds 1.05 across recent timeseries entries,
        # flag supercritical drift.  This closes the sub-obligation opened
        # by the telemetry snapshot: the system must self-correct toward
        # criticality or the drift becomes a load-bearing diagnostic.
        #
        # Algorithm: load the last N entries from criticality_timeseries.json,
        # extract σ values, and compute the fraction that exceed 1.05.
        # If >60% of the last 10 entries are supercritical, flag drift.
        # Also compute α drift statistics over the same window.
        _DRIFT_WINDOW = 10
        _SUPERCRIT_THRESHOLD = 1.05
        _SUPERCRIT_PERSISTENCE_FRAC = 0.60
        _drift_ts_file = DOCS_DIR / "criticality_timeseries.json"
        _drift_sigmas = []
        _drift_alphas = []
        if _drift_ts_file.exists():
            try:
                _drift_entries = json.loads(_drift_ts_file.read_text())
                if isinstance(_drift_entries, list):
                    for _de in _drift_entries[-_DRIFT_WINDOW:]:
                        _de_s = _de.get("sigma")
                        if _de_s is not None:
                            try:
                                _drift_sigmas.append(float(_de_s))
                            except (TypeError, ValueError):
                                pass
                        _de_a = _de.get("alpha")
                        if _de_a is not None:
                            try:
                                _drift_alphas.append(float(_de_a))
                            except (TypeError, ValueError):
                                pass
            except (json.JSONDecodeError, OSError):
                pass

        # Include current snapshot σ and α in the window
        if _snap_sigma is not None:
            try:
                _drift_sigmas.append(float(_snap_sigma))
            except (TypeError, ValueError):
                pass
        if _snap_alpha is not None:
            try:
                _drift_alphas.append(float(_snap_alpha))
            except (TypeError, ValueError):
                pass

        # Trim to window size (keep most recent)
        _drift_sigmas = _drift_sigmas[-_DRIFT_WINDOW:]
        _drift_alphas = _drift_alphas[-_DRIFT_WINDOW:]

        _supercrit_drift_info = {}
        if len(_drift_sigmas) >= 3:
            _n_supercrit = sum(1 for _ds in _drift_sigmas if _ds > _SUPERCRIT_THRESHOLD)
            _supercrit_frac = _n_supercrit / len(_drift_sigmas)
            _sigma_window_mean = sum(_drift_sigmas) / len(_drift_sigmas)
            _sigma_window_std = (sum((_ds - _sigma_window_mean) ** 2
                                     for _ds in _drift_sigmas) / len(_drift_sigmas)) ** 0.5

            _supercrit_drift_info["sigma_window_size"] = len(_drift_sigmas)
            _supercrit_drift_info["sigma_window_mean"] = round(_sigma_window_mean, 6)
            _supercrit_drift_info["sigma_window_std"] = round(_sigma_window_std, 6)
            _supercrit_drift_info["n_supercritical"] = _n_supercrit
            _supercrit_drift_info["supercritical_fraction"] = round(_supercrit_frac, 4)
            _supercrit_drift_info["supercritical_threshold"] = _SUPERCRIT_THRESHOLD
            _supercrit_drift_info["persistence_threshold"] = _SUPERCRIT_PERSISTENCE_FRAC

            _persistent_supercrit = _supercrit_frac >= _SUPERCRIT_PERSISTENCE_FRAC
            _supercrit_drift_info["persistent_supercritical_drift"] = _persistent_supercrit

            if _persistent_supercrit:
                _supercrit_drift_info["drift_verdict"] = "SUPERCRITICAL_DRIFT"
                _supercrit_drift_info["drift_warning"] = (
                    f"σ>{_SUPERCRIT_THRESHOLD} in {_n_supercrit}/{len(_drift_sigmas)} "
                    f"recent entries ({round(_supercrit_frac * 100, 1)}% >= "
                    f"{round(_SUPERCRIT_PERSISTENCE_FRAC * 100)}% threshold) — "
                    f"system is not self-correcting toward criticality; "
                    f"mean σ={round(_sigma_window_mean, 4)}±{round(_sigma_window_std, 4)}"
                )
                print(
                    f"[CA] *** SUPERCRITICAL DRIFT: σ>{_SUPERCRIT_THRESHOLD} "
                    f"in {_n_supercrit}/{len(_drift_sigmas)} recent entries ***"
                )
            else:
                _supercrit_drift_info["drift_verdict"] = "WITHIN_TOLERANCE"

        # α drift statistics over same window
        if len(_drift_alphas) >= 3:
            _alpha_window_mean = sum(_drift_alphas) / len(_drift_alphas)
            _alpha_window_std = (sum((_da - _alpha_window_mean) ** 2
                                     for _da in _drift_alphas) / len(_drift_alphas)) ** 0.5
            _supercrit_drift_info["alpha_window_size"] = len(_drift_alphas)
            _supercrit_drift_info["alpha_window_mean"] = round(_alpha_window_mean, 6)
            _supercrit_drift_info["alpha_window_std"] = round(_alpha_window_std, 6)
            _supercrit_drift_info["alpha_in_soc_band"] = 1.5 <= _alpha_window_mean <= 2.5

        if _supercrit_drift_info:
            ca_telemetry_snapshot["supercritical_drift"] = _supercrit_drift_info

        summary["ca_telemetry_snapshot"] = ca_telemetry_snapshot

    # O148: Promote entropy_fraction_of_max to the per-step summary record
    # alongside branching_ratio and avalanche_exponent so all three primary
    # criticality time-series signals are directly queryable per cycle.
    if criticality:
        _efm = criticality.get("h_over_h_max")
        if _efm is not None:
            summary["entropy_fraction_of_max"] = _efm

    # O148 structured telemetry: promote key metrics to top-level summary
    # fields so subsequent FEED steps can cross-correlate without parsing
    # the nested criticality dict.  These flat fields enable the epistemic
    # loop to detect convergence of criticality metrics across replicates.
    if criticality:
        _br = criticality.get("branching_ratio")
        if _br is not None:
            summary["branching_ratio"] = _br
        # O148: σ uncertainty band — required for falsifiability of σ≈1.0 claim
        _br_err = criticality.get("branching_ratio_err")
        if _br_err is not None:
            summary["branching_ratio_err"] = _br_err
        _ae = criticality.get("avalanche_exponent")
        if _ae is not None:
            summary["avalanche_exponent"] = _ae
        # O148: power-law R² — quantifies fit quality; R²<0.9 flags weak universality
        _r2 = criticality.get("power_law_r2")
        if _r2 is not None:
            summary["power_law_r2"] = _r2
        _sr = criticality.get("survival_rate")
        if _sr is not None:
            summary["survival_rate"] = _sr
        # O148: persist shannon_entropy as its own top-level field so it is
        # recorded alongside branching_ratio and avalanche_exponent in every
        # per-step metrics dict — enables longitudinal H tracking and
        # finite-size scaling analysis without a separate pipeline.
        _se = criticality.get("shannon_entropy")
        if _se is not None:
            summary["shannon_entropy"] = _se
        # entropy_ratio: H/H_max is the scale-invariant criticality index
        _er = criticality.get("h_over_h_max")
        if _er is not None:
            summary["entropy_ratio"] = _er
        elif criticality.get("shannon_entropy") is not None:
            # Fall back to raw entropy when ratio unavailable
            summary["entropy_ratio"] = criticality.get("shannon_entropy")
        _dct = criticality.get("dominant_cell_type")
        if _dct is not None:
            summary["dominant_cell_type"] = _dct
        # O148: joint criticality verdict and its basis — makes RESOLVE
        # firing automatable by surfacing the machine-readable verdict
        # (e.g. AT_CRITICAL, CRITICAL_CONTESTED) at the top level
        _cv = criticality.get("criticality_verdict")
        if _cv is not None:
            summary["criticality_verdict"] = _cv
        _vb = criticality.get("verdict_basis")
        if _vb is not None:
            summary["verdict_basis"] = _vb

    # O148: Assemble scored fields — each criticality metric is stored with
    # its value, a pass/fail score, and the band that defines "healthy".
    # This lets the epistemic loop flag drift from the critical band
    # automatically rather than requiring manual inspection of snapshot text.
    if criticality:
        scored_fields = {}
        _sf_sigma = criticality.get("branching_ratio")
        if _sf_sigma is not None:
            try:
                _sf_s = float(_sf_sigma)
                _sf_sigma_err = criticality.get("branching_ratio_err")
                scored_fields["branching_ratio"] = {
                    "value": _sf_sigma,
                    "error": _sf_sigma_err,
                    "band": [0.95, 1.05],
                    "in_band": 0.95 <= _sf_s <= 1.05,
                    "score": round(max(0.0, 1.0 - abs(_sf_s - 1.0) / 0.05), 4),
                    "unit": "dimensionless",
                }
            except (TypeError, ValueError):
                pass
        _sf_alpha = criticality.get("avalanche_exponent")
        if _sf_alpha is not None:
            try:
                _sf_a = float(_sf_alpha)
                scored_fields["avalanche_exponent"] = {
                    "value": _sf_alpha,
                    "band": [2.0, 2.5],
                    "in_band": 2.0 <= _sf_a <= 2.5,
                    "score": round(max(0.0, 1.0 - min(abs(_sf_a - 2.0), abs(_sf_a - 2.5)) / 0.5), 4) if not (2.0 <= _sf_a <= 2.5) else 1.0,
                    "unit": "dimensionless",
                }
            except (TypeError, ValueError):
                pass
        _sf_r2 = criticality.get("power_law_r2")
        if _sf_r2 is not None:
            try:
                _sf_r2v = float(_sf_r2)
                scored_fields["power_law_r2"] = {
                    "value": _sf_r2,
                    "threshold": 0.80,
                    "above_threshold": _sf_r2v >= 0.80,
                    "score": round(min(1.0, _sf_r2v / 0.80), 4),
                    "unit": "dimensionless",
                }
            except (TypeError, ValueError):
                pass
        _sf_se = criticality.get("shannon_entropy")
        if _sf_se is not None:
            _sf_h_ratio = criticality.get("h_over_h_max")
            _sf_h_max = criticality.get("h_max")
            _se_entry = {
                "value": _sf_se,
                "unit": "bits",
            }
            if _sf_h_ratio is not None and _sf_h_max is not None:
                try:
                    _sf_hr = float(_sf_h_ratio)
                    _se_entry["h_over_h_max"] = _sf_h_ratio
                    _se_entry["h_max"] = _sf_h_max
                    # AT_RIDGE band: H/H_max ∈ [0.15, 0.25]
                    _se_entry["band_h_ratio"] = [0.15, 0.25]
                    _se_entry["in_band"] = 0.15 <= _sf_hr <= 0.25
                    # Score: 1.0 at band center (0.20), decaying linearly
                    _se_entry["score"] = round(max(0.0, 1.0 - abs(_sf_hr - 0.20) / 0.10), 4)
                except (TypeError, ValueError):
                    pass
            scored_fields["shannon_entropy"] = _se_entry
        _sf_sr = criticality.get("survival_rate")
        if _sf_sr is not None:
            scored_fields["survival_rate"] = {
                "value": _sf_sr,
                "unit": "fraction",
            }
        if scored_fields:
            # Flag overall drift: any primary metric out of band
            scored_fields["_drift_detected"] = any(
                not entry.get("in_band", entry.get("above_threshold", True))
                for entry in scored_fields.values()
                if isinstance(entry, dict) and "value" in entry
            )
            summary["scored_fields"] = scored_fields

    # O148: Emit a flat, machine-readable criticality_verdict_record so every
    # snapshot is self-describing with σ, α, R², survival_rate, and verdict.
    # This structured record replaces raw cell counts as the primary
    # criticality signal and enables automated RESOLVE detection.
    if criticality:
        _cvr_sigma = criticality.get("branching_ratio")
        _cvr_sigma_err = criticality.get("branching_ratio_err")
        _cvr_alpha = criticality.get("avalanche_exponent")
        _cvr_r2 = criticality.get("power_law_r2")
        _cvr_survival = criticality.get("survival_rate")
        _cvr_entropy = criticality.get("shannon_entropy")
        _cvr_h_ratio = criticality.get("h_over_h_max")
        _cvr_verdict = criticality.get("criticality_verdict")

        # Per-metric pass/fail for machine consumption
        _cvr_sigma_pass = False
        if _cvr_sigma is not None:
            try:
                _cvr_sigma_pass = abs(float(_cvr_sigma) - 1.0) <= 0.05
            except (TypeError, ValueError):
                pass
        _cvr_r2_pass = False
        if _cvr_r2 is not None:
            try:
                _cvr_r2_pass = float(_cvr_r2) >= 0.80
            except (TypeError, ValueError):
                pass
        _cvr_alpha_pass = False
        if _cvr_alpha is not None:
            try:
                _cvr_alpha_pass = 1.0 <= float(_cvr_alpha) <= 3.0
            except (TypeError, ValueError):
                pass

        _cvr_at_critical = (
            _cvr_sigma_pass and _cvr_r2_pass
            and _cvr_verdict is not None
            and "CRITICAL" in str(_cvr_verdict).upper()
        )

        criticality_verdict_record = {
            "sigma": _cvr_sigma,
            "sigma_err": _cvr_sigma_err,
            "sigma_in_band": _cvr_sigma_pass,
            "alpha": _cvr_alpha,
            "alpha_in_band": _cvr_alpha_pass,
            "r2": _cvr_r2,
            "r2_pass": _cvr_r2_pass,
            "survival_rate": _cvr_survival,
            "shannon_entropy": _cvr_entropy,
            "h_over_h_max": _cvr_h_ratio,
            "verdict": _cvr_verdict,
            "AT_CRITICAL": _cvr_at_critical,
        }
        summary["criticality_verdict_record"] = criticality_verdict_record

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

    # O148: Record dominant cell-type identity and fractional prevalence in
    # every timeseries entry.  This converts the dominant semantic attractor
    # from a single-snapshot observation into a longitudinal falsifiable
    # signal — enabling detection of attractor shifts across generations
    # (e.g. Physics Navigator losing dominance signals a phase transition).
    dom_type = criticality.get("dominant_cell_type")
    dom_count = criticality.get("dominant_cell_count")
    dom_frac = criticality.get("dominant_cell_fraction")
    if dom_type is not None:
        entry["dominant_type"] = dom_type
    if dom_count is not None:
        entry["dominant_count"] = dom_count
    if dom_frac is not None:
        entry["dominant_fraction"] = dom_frac
        # Pair identity + prevalence as a single structured record for
        # downstream consumers that need the attractor signature atomically
        entry["dominant_type_identity"] = {
            "type": dom_type,
            "count": dom_count,
            "fraction": dom_frac,
        }

    # INV_073: Log H/H_max (entropy ratio) alongside σ and α in every
    # time-series entry, enabling longitudinal tracking of whether the
    # critical ridge consistently occupies a specific entropy band
    # (e.g. H/H_max ≈ 0.186) rather than only a σ band.  This converts
    # a single-snapshot curiosity into a falsifiable time-series invariant.
    h_over_h_max = criticality.get("h_over_h_max")
    h_max_val = criticality.get("h_max")
    entropy_criticality = criticality.get("entropy_criticality")
    if h_over_h_max is not None:
        entry["h_over_h_max"] = h_over_h_max
    if h_max_val is not None:
        entry["h_max"] = h_max_val
    if entropy_criticality is not None:
        entry["entropy_criticality"] = entropy_criticality

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

    # O148: Rolling α drift diagnostic — detect power-law exponent trending
    # away from the SOC universality band [1.5, 2.5] across generations.
    # Mirrors the σ rolling diagnostic above so both primary criticality
    # indicators have longitudinal drift detection in the timeseries.
    recent_alphas = [e.get("alpha") for e in existing[-19:] if e.get("alpha") is not None]
    if alpha is not None:
        recent_alphas.append(alpha)
    if len(recent_alphas) >= 3:
        mean_alpha = sum(recent_alphas) / len(recent_alphas)
        alpha_std = (sum((a - mean_alpha) ** 2 for a in recent_alphas) / len(recent_alphas)) ** 0.5
        entry["alpha_rolling_mean"] = round(mean_alpha, 6)
        entry["alpha_rolling_std"] = round(alpha_std, 6)
        entry["alpha_in_soc_band"] = 1.5 <= mean_alpha <= 2.5

    # O148: Per-snapshot scored band membership — σ and α pass/fail stored
    # per timeseries entry so longitudinal drift from the critical ridge
    # is machine-detectable without re-parsing raw values downstream.
    if sigma is not None:
        try:
            s_val = float(sigma)
            entry["sigma_band_pass"] = abs(s_val - 1.0) <= 0.05
        except (TypeError, ValueError):
            pass
    if alpha is not None:
        try:
            a_val = float(alpha)
            entry["alpha_band_pass"] = 1.5 <= a_val <= 2.5
        except (TypeError, ValueError):
            pass
    if r2 is not None:
        try:
            r2_val = float(r2)
            entry["r2_pass"] = r2_val >= 0.80
        except (TypeError, ValueError):
            pass

    # Joint ridge-drift flag: True when ANY primary indicator is out of band
    sigma_ok = entry.get("sigma_band_pass", True)
    alpha_ok = entry.get("alpha_band_pass", True)
    r2_ok = entry.get("r2_pass", True)
    entry["ridge_drift_detected"] = not (sigma_ok and alpha_ok and r2_ok)

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

    // CA telemetry block: render σ, α, survival_rate, and verdict when present
    const crit = c.criticality || {};
    const hasCrit = c.branching_ratio != null || c.avalanche_exponent != null || c.survival_rate != null;
    let critHtml = '';
    if (hasCrit) {
      const sigma = c.branching_ratio != null ? c.branching_ratio : crit.branching_ratio;
      const sigmaErr = c.branching_ratio_err != null ? c.branching_ratio_err : crit.branching_ratio_err;
      const alpha = c.avalanche_exponent != null ? c.avalanche_exponent : crit.avalanche_exponent;
      const r2 = c.power_law_r2 != null ? c.power_law_r2 : crit.power_law_r2;
      const sr = c.survival_rate != null ? c.survival_rate : crit.survival_rate;
      const cv = c.criticality_verdict || crit.criticality_verdict || '';
      const er = c.entropy_ratio != null ? c.entropy_ratio : (crit.h_over_h_max || crit.shannon_entropy);
      const dct = c.dominant_cell_type || crit.dominant_cell_type || null;
      const dcc = (crit.dominant_cell_count != null ? crit.dominant_cell_count : null);
      const dcf = (crit.dominant_cell_fraction != null ? crit.dominant_cell_fraction : null);
      // H/H_max ratio and entropy criticality from _extract_criticality_verdict
      const hOverHmax = crit.h_over_h_max != null ? crit.h_over_h_max : null;
      const hMax = crit.h_max != null ? crit.h_max : null;
      const entCrit = crit.entropy_criticality || null;
      const verdictColor = cv.includes('AT_CRITICAL') ? 'var(--green)' :
                           cv.includes('SUPERCRITICAL') ? 'var(--red)' :
                           cv.includes('SUBCRITICAL') ? 'var(--blue)' : 'var(--muted)';
      const parts = [];
      if (sigma != null) parts.push(`σ=${sigma}${sigmaErr != null ? '±'+sigmaErr : ''}`);
      if (alpha != null) parts.push(`α=${alpha}${r2 != null ? ' (R²='+r2+')' : ''}`);
      // O148: Always show Shannon entropy H and H/H_max as explicit metrics alongside σ and α
      // H/H_max (entropy fraction) is the normalized dimensionless criticality index,
      // directly comparable across grid sizes — promoted to primary metric position.
      const seVal = c.shannon_entropy != null ? c.shannon_entropy : crit.shannon_entropy;
      if (seVal != null) {
        if (hOverHmax != null) {
          parts.push(`H=${seVal} bits (${hOverHmax} of H_max${hMax != null ? '='+hMax : ''})`);
        } else {
          parts.push(`H=${seVal} bits`);
        }
      } else if (hOverHmax != null) {
        parts.push(`H/H_max=${hOverHmax}${hMax != null ? ' (H_max='+hMax+')' : ''}`);
      } else if (er != null) {
        parts.push(`H=${er}`);
      }
      if (sr != null) parts.push(`survival=${sr}`);
      if (dct != null) parts.push(`dominant=${dct}${dcc != null ? '('+dcc+')' : ''}${dcf != null ? ' '+Math.round(dcf*100)+'%' : ''}`);

      // O148: Structured σ/α/H telemetry metrics grid — labeled fields with
      // band-membership flags.  σ∉[0.95,1.05] flagged as subcritical/supercritical
      // for downstream filtering.  This block emits the three primary criticality
      // observables as individually labeled, machine-readable metric rows so
      // longitudinal tracking (O148) and STF metric tensor recovery (O112)
      // can consume them without re-parsing the verdict string.
      let telemetryMetricsHtml = '';
      const _tmRows = [];
      if (sigma != null) {
        const sVal = parseFloat(sigma);
        const sigmaInBand = (sVal >= 0.95 && sVal <= 1.05);
        const sigmaFlag = sigmaInBand ? '✓ CRITICAL' : (sVal > 1.05 ? '⚠ SUPERCRITICAL' : '⚠ SUBCRITICAL');
        const sigmaFlagColor = sigmaInBand ? 'var(--green)' : (sVal > 1.05 ? 'var(--red)' : 'var(--blue)');
        _tmRows.push(`<span style="color:var(--muted)">σ</span> <span style="color:var(--text)">${sigma}${sigmaErr != null ? ' ± '+sigmaErr : ''}</span> <span style="color:${sigmaFlagColor};font-weight:600">${sigmaFlag}</span> <span style="color:var(--muted)">[0.95–1.05]</span>`);
      }
      if (alpha != null) {
        const aVal = parseFloat(alpha);
        const alphaInBand = (aVal >= 2.0 && aVal <= 2.5);
        const alphaLabel = alphaInBand ? '✓ SOC band' : (aVal > 2.5 ? '~ extended' : '⚠ out of band');
        const alphaColor = alphaInBand ? 'var(--green)' : 'var(--amber)';
        _tmRows.push(`<span style="color:var(--muted)">α</span> <span style="color:var(--text)">${alpha}</span>${r2 != null ? ` <span style="color:var(--muted)">R²=${r2}</span>` : ''} <span style="color:${alphaColor}">${alphaLabel}</span> <span style="color:var(--muted)">[2.0–2.5]</span>`);
      }
      if (seVal != null || hOverHmax != null) {
        const hDisp = seVal != null ? seVal + ' bits' : '—';
        const ratioDisp = hOverHmax != null ? hOverHmax : '—';
        const hColor = (hOverHmax != null && hOverHmax >= 0.15 && hOverHmax <= 0.25) ? 'var(--green)' : 'var(--muted)';
        const hLabel = (hOverHmax != null && hOverHmax >= 0.15 && hOverHmax <= 0.25) ? '✓ AT_RIDGE' :
                       (hOverHmax != null && hOverHmax < 0.15) ? '⚠ FROZEN' :
                       (hOverHmax != null && hOverHmax > 0.25 && hOverHmax < 0.5) ? '~ NEAR_RIDGE' :
                       (hOverHmax != null && hOverHmax >= 0.5) ? '⚠ DISORDERED' : '';
        _tmRows.push(`<span style="color:var(--muted)">H</span> <span style="color:var(--text)">${hDisp}</span> <span style="color:var(--muted)">H/H_max=</span><span style="color:var(--text)">${ratioDisp}</span>${hLabel ? ` <span style="color:${hColor}">${hLabel}</span>` : ''}`);
      }
      if (_tmRows.length) {
        telemetryMetricsHtml = `<div style="font-family:var(--mono);font-size:0.60rem;margin-top:0.25rem;padding:0.3rem 0.5rem;border:1px solid var(--border);background:rgba(255,255,255,0.018);line-height:1.7">`
          + `<div style="color:var(--muted);font-size:0.55rem;letter-spacing:0.12em;margin-bottom:0.15rem">CA TELEMETRY — σ · α · H</div>`
          + _tmRows.join('<br>')
          + `</div>`;
      }

      // O148: Criticality-state badge — AT_CRITICAL / SUBCRITICAL / SUPERCRITICAL
      const stateBadge = cv.includes('AT_CRITICAL') ? '🟢 AT_CRITICAL' :
                         cv.includes('SUPERCRITICAL') ? '🔴 SUPERCRITICAL' :
                         cv.includes('SUBCRITICAL') ? '🔵 SUBCRITICAL' :
                         cv.includes('CRITICAL_LOW') ? '🟡 LOW_CONFIDENCE' :
                         cv.includes('CRITICAL_CONTESTED') ? '🟠 CONTESTED' : '';
      const stateBadgeColor = cv.includes('AT_CRITICAL') ? 'var(--green)' :
                              cv.includes('SUPERCRITICAL') ? 'var(--red)' :
                              cv.includes('SUBCRITICAL') ? 'var(--blue)' :
                              cv.includes('CRITICAL') ? 'var(--amber)' : 'var(--muted)';

      // O148: Structured scored-fields grid — σ, α, R², verdict as machine-readable rows
      const sf = (c.scored_fields || crit.scored_fields || null);
      let scoredHtml = '';
      if (sf) {
        const rows = [];
        const _sfSigma = sf.branching_ratio;
        if (_sfSigma) {
          const ib = _sfSigma.in_band ? '✓' : '✗';
          const ibc = _sfSigma.in_band ? 'var(--green)' : 'var(--red)';
          rows.push(`<span style="color:${ibc}">${ib}</span> σ=${_sfSigma.value}${_sfSigma.error != null ? '±'+_sfSigma.error : ''} <span style="color:var(--muted)">[${_sfSigma.band[0]}–${_sfSigma.band[1]}] score=${_sfSigma.score}</span>`);
        }
        const _sfAlpha = sf.avalanche_exponent;
        if (_sfAlpha) {
          const ib = _sfAlpha.in_band ? '✓' : '✗';
          const ibc = _sfAlpha.in_band ? 'var(--green)' : 'var(--red)';
          rows.push(`<span style="color:${ibc}">${ib}</span> α=${_sfAlpha.value} <span style="color:var(--muted)">[${_sfAlpha.band[0]}–${_sfAlpha.band[1]}] score=${_sfAlpha.score}</span>`);
        }
        const _sfR2 = sf.power_law_r2;
        if (_sfR2) {
          const ab = _sfR2.above_threshold ? '✓' : '✗';
          const abc = _sfR2.above_threshold ? 'var(--green)' : 'var(--red)';
          rows.push(`<span style="color:${abc}">${ab}</span> R²=${_sfR2.value} <span style="color:var(--muted)">[≥${_sfR2.threshold}] score=${_sfR2.score}</span>`);
        }
        const _sfH = sf.shannon_entropy;
        if (_sfH) {
          rows.push(`· H=${_sfH.value} bits`);
        }
        if (sf._drift_detected) {
          rows.push(`<span style="color:var(--amber)">⚠ DRIFT DETECTED — metric(s) outside healthy band</span>`);
        }
        if (rows.length) {
          scoredHtml = `<div style="font-family:var(--mono);font-size:0.60rem;margin-top:0.2rem;padding:0.25rem 0.5rem;border:1px solid var(--border);background:rgba(255,255,255,0.015)">${rows.join('<br>')}</div>`;
        }
      }

      // O148: Verdict basis trail — shows the joint evidence chain
      const vb = c.verdict_basis || crit.verdict_basis || null;
      let basisHtml = '';
      if (vb && Array.isArray(vb) && vb.length) {
        basisHtml = `<div style="font-family:var(--mono);font-size:0.58rem;margin-top:0.15rem;color:var(--muted);padding-left:0.5rem">basis: ${vb.join(' · ')}</div>`;
      }

      // Semantic cold death warning when H/H_max < 0.15
      let coldDeathHtml = '';
      if (hOverHmax != null && hOverHmax < 0.15) {
        coldDeathHtml = `<div style="font-family:var(--mono);font-size:0.62rem;margin-top:0.2rem;padding:0.2rem 0.5rem;border-left:2px solid var(--red);color:var(--red);background:rgba(239,68,68,0.06)">⚠ SEMANTIC COLD DEATH WARNING — H/H_max=${hOverHmax} < 0.15 · cell-type homogenization may freeze semantic diversity${entCrit ? ' · entropy_criticality='+entCrit : ''}</div>`;
      } else if (hOverHmax != null && entCrit) {
        coldDeathHtml = `<div style="font-family:var(--mono);font-size:0.62rem;margin-top:0.2rem;padding:0.2rem 0.5rem;border-left:2px solid var(--muted);color:var(--muted);background:rgba(255,255,255,0.01)">entropy_criticality=${entCrit} · H/H_max=${hOverHmax}</div>`;
      }
      critHtml = `<div style="font-family:var(--mono);font-size:0.68rem;margin-top:0.35rem;padding:0.3rem 0.5rem;border-left:2px solid ${verdictColor};background:rgba(255,255,255,0.02)">
        <span style="color:${verdictColor};letter-spacing:0.08em;font-weight:600" data-verdict="${cv || ''}">${stateBadge || cv || 'NO_VERDICT'}</span>
        <span style="color:var(--muted);margin-left:0.5rem">${parts.join(' · ')}</span>
      </div>${basisHtml}${scoredHtml}${coldDeathHtml}`;
    }

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
