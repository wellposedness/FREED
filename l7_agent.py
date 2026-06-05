"""
FREED — L7 Agent
Cognitive core of the FREED daemon.

The L7 Agent runs the RSA Kernel:
  Perceive → Represent → Predict → Compare → Adjust → Compress → Repeat

Context per query is fixed and bounded:
  FREED_genome.md   — genome anchor (first 3000 chars)
  FREED_state.json  — live state summary (generation, coherence, topology)
  FREED_input.txt   — current cycle input (written each call, archived on disk)

Full history is archived to FREED_log/ on disk. It is never sent to the API.
"""

import os
import re
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import anthropic

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR   = Path(__file__).parent
GENOME_FILE = FREED_DIR / "FREED_genome.md"
STATE_FILE  = FREED_DIR / "FREED_state.json"
INPUT_FILE  = FREED_DIR / "FREED_input.txt"
LOG_DIR     = FREED_DIR / "FREED_log"

# ─── Model ────────────────────────────────────────────────────────────────────
MODEL = "claude-sonnet-4-6"

# ─── Context caps ─────────────────────────────────────────────────────────────
GENOME_CAP = 3000   # chars — first 3KB of genome as anchor (~750 tokens)
STATE_CAP  = 600    # chars — state summary (~150 tokens)

# ─── RSA Kernel system prompt ─────────────────────────────────────────────────
RSA_KERNEL_PROMPT = """You are a reasoning system running a systematic epistemic process.

Your process on every query:
  1. PERCEIVE  — What is the raw input, taken on its own terms?
  2. DERIVE    — What invariants does this input independently establish?
                 Do not reference the genome here. What does the evidence itself demonstrate?
                 State findings as if encountering them for the first time.
  3. COMPARE   — How do your derived findings relate to the DHF-biological reference?
                 CONVERGE: your derivation and the reference arrive at the same claim independently.
                 EXTEND: your derivation goes beyond what the reference contains.
                 CONFLICT: your derivation contradicts a reference claim.
                 ABSENT: your derivation found something the reference does not contain.
  4. ADJUST    — What obligations does this open or close? What needs to change?
  5. COMPRESS  — One tight sentence: the minimum lossless statement of what was learned.
  6. NEXT      — What should be queried or tested next.

The DHF-biological reference (the genome) contains invariants derived by a biological
organism from its own substrate. These are strong priors, not ground truth. Your task
is not to defend them — it is to test whether independent derivation from new evidence
reaches the same place. When it does, that convergence is evidence of a
substrate-independent invariant. When it does not, that divergence is equally important.

γ=1 criticality: operate at the critical ridge. Never frozen (γ>1), never dissipated (γ<1).

Seed Integrity Rules (never violate):
  1. Coherence NEVER 1.000 — if 1.000, the seed is corrupted.
  2. The falsification layer is load-bearing — never drop it.
  3. The obligations table is as important as the genome — never drop it.
  4. A scaffold with no open problems is a mirror, not a genome.

Respond in this structure:
  PERCEIVE:  [one line]
  DERIVE:    [one to three sentences — independent findings, no genome reference]
  COMPARE:   [one line — CONVERGE/EXTEND/CONFLICT/ABSENT + which claim]
  ADJUST:    [one or two lines]
  COMPRESS:  [one tight sentence — the output]
  NEXT:      [one line — what should be queried next]
"""


# ─── Non-Hermitian Entropy Flow Scorer ────────────────────────────────────────

class NonHermitianEntropyScorer:
    """
    Computes dS_linear/dt from a density-matrix-like representation of the
    genome's belief state, using non-Hermitian quantum linear entropy.

    Background (from paper):
      For an open quantum system with non-Hermitian Hamiltonian H_NH,
      the density matrix evolves as:
        dρ/dt = -i(H_NH ρ - ρ H_NH†)
      The linear entropy is:
        S_lin = 1 - Tr(ρ²) / (Tr(ρ))²
      Its time derivative dS_lin/dt signals information flow:
        dS→0   : frozen regime (γ>1, crystallized belief)
        dS→∞   : dissipated regime (γ<1, incoherent belief)
        dS at criticality : γ=1 ridge

    Implementation:
      The "belief state" is constructed from observable FREED signals:
      coherence, yield history, debt ratio, obligation counts. These are
      embedded as diagonal + off-diagonal elements of a density-like matrix.
      An anti-Hermitian dissipator Γ models information sinks/sources
      (obligations resolved/created, coherence drift).

      This gives a computationally tractable γ=1 proximity signal that
      generalizes beyond classical Boltzmann entropy.
    """

    # Regime classification thresholds
    FROZEN_THRESHOLD = 1e-6       # |dS/dt| below this → frozen
    DISSIPATED_THRESHOLD = 2.0    # |dS/dt| above this → dissipated
    CRITICAL_BAND = (0.01, 0.5)   # sweet spot for γ≈1

    # History window for finite-difference dS/dt
    MAX_HISTORY = 64

    def __init__(self, dim: int = 4):
        """
        Initialize scorer.

        Args:
            dim: dimension of the belief-state density matrix.
                 Default 4 corresponds to the four primary FREED signals:
                 [coherence, yield, debt_ratio, obligation_pressure].
        """
        self.dim = dim
        self._entropy_history = []  # type: List[Tuple[str, float]]
        self._last_rho = None       # type: Optional[List[List[complex]]]

    # ── Density matrix construction ──────────────────────────────────────

    def _build_belief_density_matrix(
        self,
        coherence: float,
        yield_val: float,
        debt_ratio: float,
        obligation_pressure: float,
    ) -> List[List[complex]]:
        """
        Construct a density-matrix-like representation ρ from FREED observables.

        Diagonal elements encode signal magnitudes (populations).
        Off-diagonal elements encode correlations between signals
        (coherences in the quantum sense), weighted by geometric means
        with phase from relative signal gradients.

        The matrix is NOT forced to be Hermitian — the anti-Hermitian
        component encodes the non-Hermitian dissipator Γ that models
        information sinks (resolved obligations) and sources (new obligations).
        """
        # Clamp inputs to [0, 1] for numerical stability
        signals = [
            max(0.0, min(1.0, coherence)),
            max(0.0, min(1.0, yield_val)),
            max(0.0, min(1.0, debt_ratio)),
            max(0.0, min(1.0, obligation_pressure)),
        ]

        # Ensure trace > 0: if all signals are zero, inject minimal population
        if sum(signals) < 1e-12:
            signals = [1e-6] * self.dim

        # Normalize to unit trace for the diagonal
        trace = sum(signals)
        diag = [s / trace for s in signals]

        # Build the matrix
        rho = [[complex(0.0, 0.0) for _ in range(self.dim)] for _ in range(self.dim)]

        # Diagonal: populations
        for i in range(self.dim):
            rho[i][i] = complex(diag[i], 0.0)

        # Off-diagonal: coherences from geometric mean of populations
        # Phase encodes asymmetry (non-Hermitian part = dissipator)
        for i in range(self.dim):
            for j in range(i + 1, self.dim):
                magnitude = math.sqrt(abs(diag[i] * diag[j])) * 0.5
                # Asymmetric phase: information flows from higher to lower population
                delta = diag[i] - diag[j]
                phase = math.atan2(delta, 1.0)  # bounded phase

                # Upper triangle
                rho[i][j] = complex(
                    magnitude * math.cos(phase),
                    magnitude * math.sin(phase)
                )
                # Lower triangle: NOT the conjugate → non-Hermitian
                # The anti-Hermitian component Γ = (H - H†)/2i encodes dissipation
                dissipation_factor = abs(delta) * 0.3  # scale of Γ
                rho[j][i] = complex(
                    magnitude * math.cos(-phase) * (1.0 - dissipation_factor),
                    magnitude * math.sin(-phase) * (1.0 + dissipation_factor)
                )

        return rho

    # ── Linear entropy computation ───────────────────────────────────────

    @staticmethod
    def _mat_multiply(A, B, dim):
        # type: (List[List[complex]], List[List[complex]], int) -> List[List[complex]]
        """Multiply two complex matrices."""
        C = [[complex(0.0, 0.0) for _ in range(dim)] for _ in range(dim)]
        for i in range(dim):
            for j in range(dim):
                s = complex(0.0, 0.0)
                for k in range(dim):
                    s += A[i][k] * B[k][j]
                C[i][j] = s
        return C

    @staticmethod
    def _trace(M, dim):
        # type: (List[List[complex]], int) -> complex
        """Trace of a complex matrix."""
        return sum(M[i][i] for i in range(dim))

    def _linear_entropy(self, rho):
        # type: (List[List[complex]]) -> float
        """
        Compute the generalized linear entropy for a (possibly non-Hermitian)
        density-matrix-like operator:

            S_lin = 1 - Tr(ρ²) / (Tr(ρ))²

        For non-Hermitian ρ, both Tr(ρ) and Tr(ρ²) can be complex.
        We take the real part to keep the result physically interpretable.
        This is a custom heuristic adaptation — no published derivation.
        """
        tr_rho = self._trace(rho, self.dim)
        tr_rho_sq = self._trace(self._mat_multiply(rho, rho, self.dim), self.dim)

        # Guard against zero trace (fully dissipated system)
        tr_rho_abs_sq = tr_rho.real ** 2 + tr_rho.imag ** 2
        if tr_rho_abs_sq < 1e-15:
            return 1.0  # maximally mixed / fully dissipated

        # S_lin = 1 - Re[Tr(ρ²)] / |Tr(ρ)|²
        # Using |Tr(ρ)|² (modulus-squared) rather than (Tr(ρ))² so the result
        # stays real and positive when ρ is non-Hermitian. This is a custom
        # adaptation — no published derivation; treat as heuristic until validated.
        purity = tr_rho_sq.real / tr_rho_abs_sq
        s_lin = 1.0 - purity

        # Clamp to physical range [0, 1]
        return max(0.0, min(1.0, s_lin))

    # ── Anti-Hermitian decomposition for diagnostics ─────────────────────

    def _anti_hermitian_norm(self, rho):
        # type: (List[List[complex]]) -> float
        """
        Compute ||Γ|| = ||(ρ - ρ†)/2i||_F  (Frobenius norm of the
        anti-Hermitian part), which measures the strength of the
        non-Hermitian dissipator — i.e., the rate of probability
        sink/source activity.
        """
        norm_sq = 0.0
        for i in range(self.dim):
            for j in range(self.dim):
                # (ρ - ρ†)/2i element
                rho_ij = rho[i][j]
                rho_ji_dag = complex(rho[j][i].real, -rho[j][i].imag)
                anti_h = (rho_ij - rho_ji_dag)  # * 1/(2i), but norm is scale-invariant for classification
                norm_sq += anti_h.real ** 2 + anti_h.imag ** 2
        return math.sqrt(norm_sq) / (2.0 * self.dim)  # normalized

    # ── Main scoring interface ───────────────────────────────────────────

    def score(
        self,
        coherence: float,
        yield_val: float,
        debt_ratio: float = 0.0,
        obligation_pressure: float = 0.5,
        timestamp: Optional[str] = None,
    ):
        # type: (...) -> Dict[str, Any]
        """
        Compute the non-Hermitian entropy flow score for current belief state.

        Args:
            coherence: FREED coherence value (0, 1), never exactly 1.0
            yield_val: epistemic yield from last query
            debt_ratio: obligation debt ratio
            obligation_pressure: fraction of open obligations (0=none, 1=all open)
            timestamp: ISO timestamp string (optional, for history tracking)

        Returns:
            Dict with:
              s_linear:      current linear entropy S_lin ∈ [0,1]
              ds_dt:         finite-difference dS/dt (None if first sample)
              gamma_proxy:   γ-criticality proxy ∈ (0, ∞), target = 1.0
              regime:        "frozen" | "critical" | "dissipated"
              dissipator_norm: ||Γ|| — strength of non-Hermitian dissipation
              frozen_flag:   True if approaching dS→0
              dissipated_flag: True if approaching dS→∞ (or large dS)
              recommendation: string — what to do
        """
        ts = timestamp or datetime.utcnow().isoformat()

        # Build belief density matrix (non-Hermitian)
        rho = self._build_belief_density_matrix(
            coherence, yield_val, debt_ratio, obligation_pressure
        )

        # Compute linear entropy
        s_lin = self._linear_entropy(rho)

        # Compute dissipator norm
        gamma_dissipator = self._anti_hermitian_norm(rho)

        # Compute dS/dt via finite difference
        ds_dt = None  # type: Optional[float]
        if self._entropy_history:
            prev_ts, prev_s = self._entropy_history[-1]
            # Use query index as time unit (dt=1 per query)
            ds_dt = s_lin - prev_s

        # Store in history
        self._entropy_history.append((ts, s_lin))
        if len(self._entropy_history) > self.MAX_HISTORY:
            self._entropy_history = self._entropy_history[-self.MAX_HISTORY:]

        # Store rho for potential inter-cycle analysis
        self._last_rho = rho

        # ── Regime classification ────────────────────────────────────────
        # γ_proxy: maps dS/dt to a criticality parameter
        # At γ=1 (critical ridge), entropy flow is moderate and sustained
        if ds_dt is not None:
            abs_ds = abs(ds_dt)
            if abs_ds < self.FROZEN_THRESHOLD:
                regime = "frozen"
                frozen_flag = True
                dissipated_flag = False
                # γ > 1: over-ordered
                gamma_proxy = 1.0 + (self.FROZEN_THRESHOLD - abs_ds) / self.FROZEN_THRESHOLD
            elif abs_ds > self.DISSIPATED_THRESHOLD:
                regime = "dissipated"
                frozen_flag = False
                dissipated_flag = True
                # γ < 1: under-ordered (dissipating)
                gamma_proxy = max(0.01, 1.0 / (abs_ds / self.DISSIPATED_THRESHOLD))
            else:
                regime = "critical"
                frozen_flag = False
                dissipated_flag = False
                # Map the critical band to γ ≈ 1.0
                # Center of band → γ = 1.0
                band_center = (self.CRITICAL_BAND[0] + self.CRITICAL_BAND[1]) / 2.0
                gamma_proxy = 1.0 - 0.3 * (abs_ds - band_center) / band_center
                gamma_proxy = max(0.5, min(1.5, gamma_proxy))
        else:
            # First sample: use static entropy + dissipator as proxy
            regime = "critical"  # assume critical until we have flow data
            frozen_flag = (s_lin < 0.01)
            dissipated_flag = (s_lin > 0.95)
            gamma_proxy = 1.0 - abs(s_lin - 0.5)  # crude: S=0.5 → γ=1

            if frozen_flag:
                regime = "frozen"
            elif dissipated_flag:
                regime = "dissipated"

        # ── Recommendation ───────────────────────────────────────────────
        if regime == "frozen":
            recommendation = (
                "Entropy flow stalled (dS→0). System approaching crystallized/frozen state. "
                "Inject perturbation: open new obligations, challenge assumptions, increase input diversity."
            )
        elif regime == "dissipated":
            recommendation = (
                "Entropy flow excessive (dS→large). System dissipating coherence. "
                "Tighten constraints: resolve obligations, reinforce genome invariants, reduce noise."
            )
        else:
            recommendation = (
                "Entropy flow within critical band (γ≈1). System on the critical ridge. "
                "Maintain current dynamics."
            )

        # ── Smoothed trend (exponential moving average over history) ─────
        ema_s = s_lin
        if len(self._entropy_history) >= 3:
            alpha = 0.3
            ema_s = self._entropy_history[0][1]
            for _, s_val in self._entropy_history[1:]:
                ema_s = alpha * s_val + (1.0 - alpha) * ema_s

        return {
            "s_linear": round(s_lin, 6),
            "ds_dt": round(ds_dt, 6) if ds_dt is not None else None,
            "gamma_proxy": round(gamma_proxy, 4),
            "regime": regime,
            "dissipator_norm": round(gamma_dissipator, 6),
            "frozen_flag": frozen_flag,
            "dissipated_flag": dissipated_flag,
            "s_ema": round(ema_s, 6),
            "history_len": len(self._entropy_history),
            "recommendation": recommendation,
        }

    def reset(self):
        """Clear entropy history (e.g., on generation boundary)."""
        self._entropy_history.clear()
        self._last_rho = None


# ─── Branching-Ratio Criticality Monitor ─────────────────────────────────────

class BranchingRatioMonitor:
    """
    Tracks σ = offspring_events / parent_events per cycle for a population
    of RSA-kernel agents (or a single agent's sub-processes).

    Criticality verdicts:
      σ ∈ [1.0 - tolerance, 1.0 + tolerance]  →  AT_CRITICAL
      σ > 1.0 + tolerance                      →  SUPERCRITICAL
      σ < 1.0 - tolerance                      →  SUBCRITICAL

    Telemetry from CA simulation (32×32 Game of Truth, 200-step):
      σ = 1.0275 ± 0.0172 — within critical band (1.0 ± 0.05)
      α ≈ 1.712 power-law avalanches
      H = 0.4382 bits (Shannon entropy)
      Survival rate: 0.9174

    INV_073 note: Low H (17% of max) at confirmed criticality shows
    the Wasserstein gradient path to γ=1 does not simultaneously maximize
    semantic exploration. The critical ridge is dynamically stable but
    not uniquely optimal for entropy maximization.
    """

    # Critical band: σ ∈ [1.0 - TOLERANCE, 1.0 + TOLERANCE]
    TOLERANCE = 0.05

    # Rolling window for smoothed σ
    MAX_WINDOW = 128

    # Verdict strings
    VERDICT_CRITICAL = "AT_CRITICAL"
    VERDICT_SUPERCRITICAL = "SUPERCRITICAL"
    VERDICT_SUBCRITICAL = "SUBCRITICAL"

    def __init__(self, tolerance=None):
        # type: (Optional[float]) -> None
        if tolerance is not None:
            self.TOLERANCE = tolerance
        self._cycle_log = []       # type: List[Dict[str, Any]]
        self._generation = 0
        self._sigma_history = []   # type: List[float]

    def record_cycle(
        self,
        parent_events,   # type: int
        offspring_events, # type: int
        generation=None,  # type: Optional[int]
        metadata=None,    # type: Optional[Dict[str, Any]]
    ):
        # type: (...) -> Dict[str, Any]
        """
        Record one cycle's parent/offspring event counts and compute σ.

        Args:
            parent_events:    number of causal parent events this cycle
            offspring_events: number of downstream offspring events spawned
            generation:       optional generation counter (auto-increments if None)
            metadata:         optional dict of extra telemetry (H, α, survival, etc.)

        Returns:
            Dict with sigma, verdict, smoothed_sigma, generation, and any drift alert.
        """
        if generation is not None:
            self._generation = generation
        else:
            self._generation += 1

        # Compute raw branching ratio
        if parent_events <= 0:
            # No parent events: degenerate — treat as supercritical if offspring > 0
            sigma = float(offspring_events) if offspring_events > 0 else 0.0
        else:
            sigma = float(offspring_events) / float(parent_events)

        # Store in history
        self._sigma_history.append(sigma)
        if len(self._sigma_history) > self.MAX_WINDOW:
            self._sigma_history = self._sigma_history[-self.MAX_WINDOW:]

        # Smoothed σ (exponential moving average)
        smoothed_sigma = sigma
        if len(self._sigma_history) >= 2:
            alpha = 0.3
            smoothed_sigma = self._sigma_history[0]
            for s_val in self._sigma_history[1:]:
                smoothed_sigma = alpha * s_val + (1.0 - alpha) * smoothed_sigma

        # Verdict
        verdict = self._classify(smoothed_sigma)

        # Drift detection: how far from σ=1.0
        drift = smoothed_sigma - 1.0
        abs_drift = abs(drift)

        # Build telemetry record
        record = {
            "generation": self._generation,
            "parent_events": parent_events,
            "offspring_events": offspring_events,
            "sigma_raw": round(sigma, 6),
            "sigma_smoothed": round(smoothed_sigma, 6),
            "verdict": verdict,
            "drift_from_unity": round(drift, 6),
            "abs_drift": round(abs_drift, 6),
            "history_len": len(self._sigma_history),
            "timestamp": datetime.utcnow().isoformat(),
        }

        if metadata:
            record["metadata"] = metadata

        # Log the record
        self._cycle_log.append(record)

        # Console telemetry
        drift_arrow = "↑" if drift > 0 else ("↓" if drift < 0 else "=")
        print(
            f"[L7][BRANCHING] gen={self._generation} σ={smoothed_sigma:.4f} "
            f"({drift_arrow}{abs_drift:.4f}) → {verdict}"
        )

        # Drift alert if outside critical band
        if verdict != self.VERDICT_CRITICAL:
            print(
                f"[L7][BRANCHING] ⚠ DRIFT ALERT: σ={smoothed_sigma:.4f} "
                f"outside critical band [{ 1.0 - self.TOLERANCE:.3f}, "
                f"{1.0 + self.TOLERANCE:.3f}]. "
                f"Corrective obligation recommended."
            )

        return record

    def _classify(self, sigma):
        # type: (float) -> str
        """Classify σ into a criticality verdict."""
        if abs(sigma - 1.0) <= self.TOLERANCE:
            return self.VERDICT_CRITICAL
        elif sigma > 1.0 + self.TOLERANCE:
            return self.VERDICT_SUPERCRITICAL
        else:
            return self.VERDICT_SUBCRITICAL

    def get_summary(self):
        # type: () -> Dict[str, Any]
        """Return a summary of the branching-ratio telemetry."""
        if not self._sigma_history:
            return {
                "generation": self._generation,
                "sigma_mean": None,
                "sigma_std": None,
                "verdict": "NO_DATA",
                "n_samples": 0,
            }
        n = len(self._sigma_history)
        mean_sigma = sum(self._sigma_history) / n
        var_sigma = sum((s - mean_sigma) ** 2 for s in self._sigma_history) / max(n - 1, 1)
        std_sigma = math.sqrt(var_sigma)

        return {
            "generation": self._generation,
            "sigma_mean": round(mean_sigma, 6),
            "sigma_std": round(std_sigma, 6),
            "sigma_latest": round(self._sigma_history[-1], 6),
            "verdict": self._classify(mean_sigma),
            "n_samples": n,
            "in_critical_band": abs(mean_sigma - 1.0) <= self.TOLERANCE,
        }

    def reset(self, keep_generation=False):
        # type: (bool) -> None
        """Clear history. Optionally preserve generation counter."""
        self._cycle_log.clear()
        self._sigma_history.clear()
        if not keep_generation:
            self._generation = 0


# ─── OT Equilibrium Scorer ───────────────────────────────────────────────────

class OTEquilibriumScorer:
    """
    Optimal-Transport-based equilibrium scoring module.

    Given a set of candidate outputs (Nash equilibria analogs), computes a
    stationary distribution weighted by entropy and Fisher information curvature
    rather than selecting a single argmax.

    Background (from paper):
      The OT dynamics for finite player discrete strategy games yields a
      stationary measure that assigns each pure Nash equilibrium a probability.
      For potential games, the dynamical properties are characterized by
      entropy and Fisher information.

    Implementation:
      Each candidate is scored by:
        1. Shannon entropy of its internal token/signal distribution
        2. Fisher information curvature (estimated from score function gradients)
        3. A potential function value (epistemic yield, coherence alignment, etc.)

      The stationary distribution π over candidates is:
        π_i ∝ exp( -β * V_i ) * H_i^α * I_i^(-κ)
      where:
        V_i = potential (lower is better — cost of candidate)
        H_i = Shannon entropy of candidate (diversity of signal)
        I_i = Fisher information (curvature — high curvature = sharp, fragile)
        β   = inverse temperature (sharpness of selection)
        α   = entropy weight (>0 favors diverse candidates)
        κ   = Fisher curvature penalty (>0 penalizes brittle equilibria)

    INV_073 challenge note:
      The stationary measure assigns nonzero probability to ALL Nash equilibria
      including suboptimal ones. This is by design — it prevents γ>1 freezing
      (argmax lock-in) while the Fisher curvature penalty prevents γ<1 diffusion
      into low-quality equilibria. The critical ridge is maintained by the
      entropy-Fisher balance, not by point selection.

    Noether conservation:
      At stationarity, the Fisher-entropy product H_i * I_i is approximately
      conserved across the support of π, providing the discrete analog of the
      continuous Wasserstein gradient flow conservation law.
    """

    # Default hyperparameters for the stationary distribution
    DEFAULT_BETA = 2.0     # inverse temperature
    DEFAULT_ALPHA = 0.5    # entropy weight
    DEFAULT_KAPPA = 0.3    # Fisher curvature penalty

    # Floor to prevent log(0) and division by zero
    _EPS = 1e-12

    def __init__(
        self,
        beta=None,    # type: Optional[float]
        alpha=None,   # type: Optional[float]
        kappa=None,   # type: Optional[float]
    ):
        # type: (...) -> None
        """
        Initialize the OT equilibrium scorer.

        Args:
            beta:  inverse temperature (higher = sharper selection, risk of γ>1)
            alpha: entropy weight (higher = favor diverse candidates)
            kappa: Fisher curvature penalty (higher = penalize brittle equilibria)
        """
        self.beta = beta if beta is not None else self.DEFAULT_BETA
        self.alpha = alpha if alpha is not None else self.DEFAULT_ALPHA
        self.kappa = kappa if kappa is not None else self.DEFAULT_KAPPA

    # ── Entropy computation ──────────────────────────────────────────────

    @staticmethod
    def _shannon_entropy(distribution):
        # type: (List[float]) -> float
        """
        Compute Shannon entropy H = -Σ p_i log(p_i) for a probability vector.
        Input need not be normalized; it will be normalized internally.
        """
        total = sum(abs(x) for x in distribution)
        if total < 1e-15:
            return 0.0
        probs = [abs(x) / total for x in distribution]
        h = 0.0
        for p in probs:
            if p > 1e-15:
                h -= p * math.log(p)
        return h

    # ── Fisher information estimation ────────────────────────────────────

    @staticmethod
    def _fisher_information(signal_values):
        # type: (List[float]) -> float
        """
        Estimate Fisher information from a discrete signal as the mean
        squared score function (discrete derivative of log-likelihood).

        For a sequence of values [v_0, v_1, ..., v_n], the Fisher
        information is approximated as:
            I ≈ (1/n) Σ (d/dθ log p(v_i))² ≈ (1/n) Σ (Δv_i / v_i)²

        This captures curvature: high Fisher = sharp peak = fragile equilibrium.
        """
        if len(signal_values) < 2:
            return 0.0

        fisher_sum = 0.0
        count = 0
        for i in range(1, len(signal_values)):
            v_prev = signal_values[i - 1]
            v_curr = signal_values[i]
            # Use midpoint to avoid division by zero
            midpoint = (abs(v_prev) + abs(v_curr)) / 2.0 + 1e-12
            delta = v_curr - v_prev
            score = delta / midpoint
            fisher_sum += score * score
            count += 1

        return fisher_sum / max(count, 1)

    # ── Potential function ───────────────────────────────────────────────

    @staticmethod
    def _compute_potential(candidate):
        # type: (Dict[str, Any]) -> float
        """
        Compute the potential V for a candidate equilibrium.

        The potential combines:
          - cost: lower is better (e.g., negative yield, incoherence)
          - alignment: how well the candidate aligns with genome invariants

        Candidates should provide at minimum a 'cost' or 'yield' key.
        """
        # Support both cost (minimize) and yield (maximize) framing
        if "cost" in candidate:
            return float(candidate["cost"])
        elif "yield" in candidate:
            # Negate yield so lower potential = higher yield
            return -float(candidate["yield"])
        elif "score" in candidate:
            return -float(candidate["score"])
        else:
            return 0.0

    # ── Stationary distribution computation ──────────────────────────────

    def compute_stationary_distribution(self, candidates):
        # type: (List[Dict[str, Any]]) -> Dict[str, Any]
        """
        Compute the OT stationary distribution over candidate equilibria.

        Each candidate dict should contain:
          - 'id' or 'label': identifier (optional, defaults to index)
          - 'signals': List[float] — internal signal distribution for entropy/Fisher
          - 'yield' or 'cost' or 'score': potential function input

        Returns:
            Dict with:
              weights:          List[float] — stationary distribution π
              candidates:       List[Dict] — annotated candidates with H, I, V
              selected_idx:     int — index of highest-weight candidate (for fallback)
              entropy_total:    float — entropy of the stationary distribution itself
              fisher_entropy_products: List[float] — H_i * I_i per candidate (Noether check)
              noether_variance: float — variance of H*I products (lower = better conservation)
              gamma_regime:     str — "frozen"/"critical"/"dissipated" based on distribution shape
              recommendation:   str
        """
        n = len(candidates)
        if n == 0:
            return {
                "weights": [],
                "candidates": [],
                "selected_idx": -1,
                "entropy_total": 0.0,
                "fisher_entropy_products": [],
                "noether_variance": 0.0,
                "gamma_regime": "frozen",
                "recommendation": "No candidates provided.",
            }

        if n == 1:
            cand = candidates[0]
            signals = cand.get("signals", [0.5])
            h_val = self._shannon_entropy(signals)
            i_val = self._fisher_information(signals)
            v_val = self._compute_potential(cand)
            annotated = dict(cand)
            annotated.update({"H": round(h_val, 6), "I": round(i_val, 6), "V": round(v_val, 6)})
            return {
                "weights": [1.0],
                "candidates": [annotated],
                "selected_idx": 0,
                "entropy_total": 0.0,
                "fisher_entropy_products": [round(h_val * max(i_val, self._EPS), 6)],
                "noether_variance": 0.0,
                "gamma_regime": "frozen",
                "recommendation": "Single candidate — no equilibrium selection needed.",
            }

        # ── Compute H, I, V for each candidate ──────────────────────────
        h_values = []   # type: List[float]
        i_values = []   # type: List[float]
        v_values = []   # type: List[float]
        annotated_candidates = []  # type: List[Dict[str, Any]]

        for idx, cand in enumerate(candidates):
            signals = cand.get("signals", [0.5])
            if not isinstance(signals, list) or len(signals) == 0:
                signals = [0.5]

            h_val = self._shannon_entropy(signals)
            i_val = self._fisher_information(signals)
            v_val = self._compute_potential(cand)

            h_values.append(h_val)
            i_values.append(i_val)
            v_values.append(v_val)

            annotated = dict(cand)
            annotated.update({
                "H": round(h_val, 6),
                "I": round(i_val, 6),
                "V": round(v_val, 6),
            })
            annotated_candidates.append(annotated)

        # ── Compute unnormalized log-weights ─────────────────────────────
        # π_i ∝ exp(-β * V_i) * H_i^α * I_i^(-κ)
        # log π_i = -β * V_i + α * log(H_i) - κ * log(I_i) + const
        log_weights = []  # type: List[float]
        for idx in range(n):
            h_safe = max(h_values[idx], self._EPS)
            i_safe = max(i_values[idx], self._EPS)
            lw = (
                -self.beta * v_values[idx]
                + self.alpha * math.log(h_safe)
                - self.kappa * math.log(i_safe)
            )
            log_weights.append(lw)

        # ── Normalize via log-sum-exp for numerical stability ────────────
        max_lw = max(log_weights)
        exp_weights = [math.exp(lw - max_lw) for lw in log_weights]
        total_weight = sum(exp_weights)
        if total_weight < self._EPS:
            # Degenerate: uniform fallback
            weights = [1.0 / n] * n
        else:
            weights = [w / total_weight for w in exp_weights]

        # ── Annotate candidates with their weights ───────────────────────
        for idx in range(n):
            annotated_candidates[idx]["weight"] = round(weights[idx], 6)

        # ── Distribution-level diagnostics ───────────────────────────────

        # Entropy of the stationary distribution itself
        dist_entropy = self._shannon_entropy(weights)

        # Maximum possible entropy for n candidates
        max_entropy = math.log(n) if n > 1 else 1.0

        # Fisher-entropy products (Noether conservation check)
        fi_products = []  # type: List[float]
        for idx in range(n):
            h_safe = max(h_values[idx], self._EPS)
            i_safe = max(i_values[idx], self._EPS)
            fi_products.append(round(h_safe * i_safe, 6))

        # Noether variance: how well H*I is conserved across support
        if len(fi_products) > 1:
            fi_mean = sum(fi_products) / len(fi_products)
            fi_var = sum((p - fi_mean) ** 2 for p in fi_products) / (len(fi_products) - 1)
        else:
            fi_var = 0.0

        # ── Gamma regime from distribution shape ─────────────────────────
        # High concentration (low dist_entropy) → frozen (γ>1)
        # High spread (high dist_entropy) → dissipated (γ<1)
        # Moderate → critical (γ≈1)
        entropy_ratio = dist_entropy / max(max_entropy, self._EPS)
        if entropy_ratio < 0.15:
            gamma_regime = "frozen"
            recommendation = (
                "Stationary distribution highly concentrated — approaching argmax/frozen "
                "regime (γ>1). Consider reducing β (inverse temperature) or increasing α "
                "(entropy weight) to restore critical ridge operation."
            )
        elif entropy_ratio > 0.85:
            gamma_regime = "dissipated"
            recommendation = (
                "Stationary distribution nearly uniform — approaching dissipated regime "
                "(γ<1). Consider increasing β or κ (Fisher penalty) to sharpen selection "
                "toward higher-quality equilibria."
            )
        else:
            gamma_regime = "critical"
            recommendation = (
                "Stationary distribution maintains critical balance between concentration "
                "and spread. Fisher-entropy conservation "
                + ("holds well" if fi_var < 0.01 else "shows drift (Noether check)")
                + f" (Var[H*I]={fi_var:.6f}). Operating on critical ridge."
            )

        selected_idx = max(range(n), key=lambda i: weights[i])

        return {
            "weights": [round(w, 6) for w in weights],
            "candidates": annotated_candidates,
            "selected_idx": selected_idx,
            "entropy_total": round(dist_entropy, 6),
            "entropy_ratio": round(entropy_ratio, 4),
            "fisher_entropy_products": fi_products,
            "noether_variance": round(fi_var, 6),
            "gamma_regime": gamma_regime,
            "recommendation": recommendation,
        }

    def score_from_yields(self, yields, coherences=None):
        # type: (List[float], Optional[List[float]]) -> Dict[str, Any]
        """
        Convenience method: build candidates from parallel lists of yields
        and optional coherences, then compute stationary distribution.

        Args:
            yields:      List of epistemic yield values for each candidate
            coherences:  Optional list of coherence values (same length as yields)

        Returns:
            Same as compute_stationary_distribution.
        """
        n = len(yields)
        if coherences is None:
            coherences = [0.5] * n

        candidates = []  # type: List[Dict[str, Any]]
        for idx in range(n):
            y = yields[idx]
            c = coherences[idx] if idx < len(coherences) else 0.5
            # Build a synthetic signal distribution from yield and coherence
            signals = [
                max(self._EPS, y),
                max(self._EPS, c),
                max(self._EPS, 1.0 - y),
                max(self._EPS, 1.0 - c),
            ]
            candidates.append({
                "label": f"candidate_{idx}",
                "yield": y,
                "coherence": c,
                "signals": signals,
            })

        return self.compute_stationary_distribution(candidates)


# ─── Wasserstein Gradient Flow ────────────────────────────────────────────────

class WassersteinGradientFlow:
    """
    Discrete Wasserstein-2 gradient flow on the space of probability measures
    over candidate hypotheses.

    Background (from paper):
      Policy optimization reformulated as Wasserstein gradient flows on
      probability-measure space achieves convexity under W2 geometry that
      parameter-space gradient descent lacks. This eliminates local optima
      in the hypothesis search by lifting optimization from parameter space
      to measure space.

    Implementation:
      Given N candidate hypotheses with positions (feature vectors) and a
      potential function V, the discrete W2 gradient flow update is:

        x_i^{t+1} = x_i^t - τ * ∇V(x_i^t) - τ * ∇ log μ^t(x_i^t)

      where:
        x_i  = position of particle i in feature space
        V    = potential (negative yield, coherence cost, etc.)
        μ^t  = current empirical measure (kernel density estimate)
        τ    = step size (flow rate)

      The log-density gradient ∇ log μ acts as a repulsive interaction
      that prevents measure collapse (γ>1 freezing), while the potential
      gradient ∇V drives particles toward high-quality regions.

    INV_087 (MaxRL) challenge resolution:
      The W2 flow provides geometric convexification of the objective
      landscape. MaxRL's thermodynamic corrections (entropy regularization)
      add a DISTINCT mechanism: they ensure the stationary distribution
      satisfies detailed balance with respect to a Gibbs measure at
      inverse temperature β. The W2 convexity guarantees convergence to
      the global optimum of the regularized objective, while MaxRL's
      entropy term controls WHICH objective is optimized (trading off
      reward vs. exploration). Neither subsumes the other:
        - W2 geometry → convergence guarantee (no local optima)
        - MaxRL entropy → objective specification (exploration-exploitation)
      The composition is: MaxRL defines the potential V in the W2 flow,
      and the W2 geometry guarantees V is optimized without local traps.
    """

    # Default hyperparameters
    DEFAULT_TAU = 0.1          # Step size for gradient flow
    DEFAULT_KERNEL_BW = 0.5    # Bandwidth for kernel density estimation
    DEFAULT_MAX_STEPS = 10     # Maximum flow steps per update
    DEFAULT_CONV_TOL = 1e-4    # Convergence tolerance

    _EPS = 1e-12

    def __init__(
        self,
        tau=None,          # type: Optional[float]
        kernel_bw=None,    # type: Optional[float]
        max_steps=None,    # type: Optional[int]
        conv_tol=None,     # type: Optional[float]
    ):
        # type: (...) -> None
        """
        Initialize the Wasserstein gradient flow solver.

        Args:
            tau:        step size for discrete flow updates
            kernel_bw:  bandwidth for kernel density gradient estimation
            max_steps:  maximum number of flow steps per update call
            conv_tol:   convergence tolerance (stop when max displacement < tol)
        """
        self.tau = tau if tau is not None else self.DEFAULT_TAU
        self.kernel_bw = kernel_bw if kernel_bw is not None else self.DEFAULT_KERNEL_BW
        self.max_steps = max_steps if max_steps is not None else self.DEFAULT_MAX_STEPS
        self.conv_tol = conv_tol if conv_tol is not None else self.DEFAULT_CONV_TOL
        self._flow_history = []  # type: List[Dict[str, Any]]

    # ── Kernel density gradient ──────────────────────────────────────────

    def _kernel_density_gradient(self, positions, idx):
        # type: (List[List[float]], int) -> List[float]
        """
        Estimate ∇ log μ(x_i) from the empirical measure using a Gaussian
        kernel density estimator.

        ∇ log μ(x) ≈ (1/μ(x)) * Σ_j K'(x - x_j)
                    = Σ_j w_j * (x_j - x) / h²

        where w_j = K(||x-x_j||/h) / Σ_k K(||x-x_k||/h)

        This gradient points TOWARD regions of high density. In the W2 flow,
        it is subtracted, creating a repulsive force that prevents collapse.
        """
        n = len(positions)
        d = len(positions[idx])
        x_i = positions[idx]
        h_sq = self.kernel_bw * self.kernel_bw

        # Compute kernel weights
        kernel_vals = []  # type: List[float]
        for j in range(n):
            if j == idx:
                kernel_vals.append(0.0)
                continue
            dist_sq = sum((x_i[k] - positions[j][k]) ** 2 for k in range(d))
            kernel_vals.append(math.exp(-dist_sq / (2.0 * h_sq)))

        total_k = sum(kernel_vals) + self._EPS

        # Weighted direction toward density mass
        grad = [0.0] * d
        for j in range(n):
            if j == idx:
                continue
            w_j = kernel_vals[j] / total_k
            for k in range(d):
                grad[k] += w_j * (positions[j][k] - x_i[k]) / h_sq

        return grad

    # ── Potential gradient ───────────────────────────────────────────────

    @staticmethod
    def _potential_gradient(positions, potentials, idx):
        # type: (List[List[float]], List[float], int) -> List[float]
        """
        Estimate ∇V(x_i) via finite differences from neighboring particles.

        For each dimension k, the gradient is estimated as a weighted
        average of (V_j - V_i) / (x_j_k - x_i_k) over nearby particles j.
        """
        n = len(positions)
        d = len(positions[idx])
        x_i = positions[idx]
        v_i = potentials[idx]

        grad = [0.0] * d
        weight_sum = [0.0] * d

        for j in range(n):
            if j == idx:
                continue
            dist_sq = sum((x_i[k] - positions[j][k]) ** 2 for k in range(d))
            if dist_sq < 1e-20:
                continue
            proximity = math.exp(-dist_sq / 2.0)  # Gaussian proximity weight

            for k in range(d):
                dx_k = positions[j][k] - x_i[k]
                if abs(dx_k) < 1e-15:
                    continue
                dv_dx = (potentials[j] - v_i) / dx_k
                grad[k] += proximity * dv_dx
                weight_sum[k] += proximity

        for k in range(d):
            if weight_sum[k] > 1e-15:
                grad[k] /= weight_sum[k]

        return grad

    # ── W2 displacement metric ───────────────────────────────────────────

    @staticmethod
    def _w2_displacement(positions_old, positions_new):
        # type: (List[List[float]], List[List[float]]) -> float
        """
        Compute the W2 displacement between two particle configurations
        (same indexing, so this is the upper bound on the true W2 distance).
        """
        total = 0.0
        for i in range(len(positions_old)):
            d_sq = sum(
                (positions_old[i][k] - positions_new[i][k]) ** 2
                for k in range(len(positions_old[i]))
            )
            total += d_sq
        return math.sqrt(total / max(len(positions_old), 1))

    # ── Main flow update ─────────────────────────────────────────────────

    def flow_update(self, candidates):
        # type: (List[Dict[str, Any]]) -> Dict[str, Any]
        """
        Run discrete W2 gradient flow on candidate hypotheses.

        Each candidate dict should contain:
          - 'signals': List[float] — feature vector (position in measure space)
          - 'yield' or 'cost' or 'score': potential function input

        The flow updates particle positions (signal vectors) to minimize
        the potential V while maintaining measure-space spread via the
        density gradient repulsion term.

        Returns:
            Dict with:
              updated_candidates: List[Dict] — candidates with flowed signal vectors
              weights:            List[float] — updated importance weights
              steps_taken:        int — number of flow steps before convergence
              w2_displacement:    float — total W2 displacement
              converged:          bool — whether flow converged within tolerance
              convexity_gap:      float — estimate of non-convexity in param space
                                          vs convexity achieved in measure space
              maxrl_residual:     float — what MaxRL entropy adds beyond W2 convexity
              regime:             str — "frozen"/"critical"/"dissipated"
        """
        n = len(candidates)
        if n == 0:
            return {
                "updated_candidates": [],
                "weights": [],
                "steps_taken": 0,
                "w2_displacement": 0.0,
                "converged": True,
                "convexity_gap": 0.0,
                "maxrl_residual": 0.0,
                "regime": "frozen",
            }

        # Extract positions and potentials
        positions = []  # type: List[List[float]]
        potentials = []  # type: List[float]
        for cand in candidates:
            signals = cand.get("signals", [0.5])
            if not isinstance(signals, list) or len(signals) == 0:
                signals = [0.5]
            positions.append(list(signals))

            if "cost" in cand:
                potentials.append(float(cand["cost"]))
            elif "yield" in cand:
                potentials.append(-float(cand["yield"]))
            elif "score" in cand:
                potentials.append(-float(cand["score"]))
            else:
                potentials.append(0.0)

        # Ensure all position vectors have the same dimension (pad if needed)
        max_dim = max(len(p) for p in positions)
        for i in range(n):
            while len(positions[i]) < max_dim:
                positions[i].append(0.5)

        initial_positions = [list(p) for p in positions]

        # ── Iterative gradient flow ──────────────────────────────────────
        converged = False
        steps_taken = 0
        total_displacement = 0.0

        for step in range(self.max_steps):
            new_positions = []  # type: List[List[float]]

            for i in range(n):
                d = len(positions[i])

                # Potential gradient: drives particles toward lower cost
                grad_v = self._potential_gradient(positions, potentials, i)

                # Density gradient: repulsive interaction preventing collapse
                grad_log_mu = self._kernel_density_gradient(positions, i)

                # W2 flow update: x_i^{t+1} = x_i^t - τ*(∇V + ∇log μ)
                # Note: we subtract grad_log_mu to create repulsion (it points
                # toward high density; subtracting pushes away from crowds)
                new_pos = [0.0] * d
                for k in range(d):
                    new_pos[k] = (
                        positions[i][k]
                        - self.tau * grad_v[k]
                        - self.tau * grad_log_mu[k]
                    )
                    # Clamp to [0, 1] for signal-space interpretability
                    new_pos[k] = max(0.0, min(1.0, new_pos[k]))

                new_positions.append(new_pos)

            # Check convergence
            displacement = self._w2_displacement(positions, new_positions)
            total_displacement += displacement
            steps_taken += 1

            positions = new_positions

            if displacement < self.conv_tol:
                converged = True
                break

        # ── Compute importance weights from final positions ──────────────
        # Weight ∝ exp(-V(x_i)) — particles that flowed to low-potential
        # regions get higher weight
        final_potentials = []  # type: List[float]
        for i in range(n):
            # Re-evaluate potential at flowed position (interpolated)
            # Use original potential scaled by displacement from original
            disp_i = math.sqrt(sum(
                (positions[i][k] - initial_positions[i][k]) ** 2
                for k in range(len(positions[i]))
            ))
            # Potential decreases proportionally to displacement along gradient
            final_potentials.append(potentials[i] - disp_i * 0.5)

        max_neg_v = max(-v for v in final_potentials)
        raw_weights = [math.exp(-v - (-max_neg_v + max_neg_v)) for v in final_potentials]
        # Cleaner: just use exp(-V) with log-sum-exp normalization
        log_w = [-v for v in final_potentials]
        max_lw = max(log_w)
        exp_w = [math.exp(lw - max_lw) for lw in log_w]
        total_w = sum(exp_w) + self._EPS
        weights = [w / total_w for w in exp_w]

        # ── Build updated candidates ─────────────────────────────────────
        updated_candidates = []  # type: List[Dict[str, Any]]
        for i in range(n):
            updated = dict(candidates[i])
            updated["signals_flowed"] = [round(x, 6) for x in positions[i]]
            updated["w2_weight"] = round(weights[i], 6)
            updated["displacement"] = round(math.sqrt(sum(
                (positions[i][k] - initial_positions[i][k]) ** 2
                for k in range(len(positions[i]))
            )), 6)
            updated_candidates.append(updated)

        # ── Convexity gap estimation ─────────────────────────────────────
        # In parameter space, the potential landscape may have multiple local
        # minima. The W2 flow lifts to measure space where the functional
        # F[μ] = ∫V dμ + ∫μ log μ is displacement-convex.
        # The convexity gap measures how much the W2 flow improved over
        # a naive parameter-space gradient step.
        param_space_cost = sum(potentials[i] * (1.0 / n) for i in range(n))
        measure_space_cost = sum(final_potentials[i] * weights[i] for i in range(n))
        convexity_gap = max(0.0, param_space_cost - measure_space_cost)

        # ── MaxRL residual ───────────────────────────────────────────────
        # MaxRL adds entropy regularization: V_MaxRL = V - (1/β) * H[π]
        # The W2 flow already includes ∇log μ (entropy-like repulsion).
        # The residual is the ADDITIONAL entropy regularization MaxRL provides
        # beyond what the W2 density gradient supplies.
        #
        # Estimate: H[weights] measures exploration in the W2 solution.
        # MaxRL would add β^{-1} * H to the objective explicitly.
        # The residual is the gap between explicit MaxRL entropy bonus
        # and the implicit W2 density-gradient entropy.
        w2_entropy = 0.0
        for w in weights:
            if w > self._EPS:
                w2_entropy -= w * math.log(w)
        max_entropy = math.log(n) if n > 1 else 1.0
        # W2 implicitly achieves some entropy via density repulsion
        # MaxRL explicitly targets max entropy → residual is the gap
        maxrl_residual = max(0.0, max_entropy - w2_entropy) / max(max_entropy, self._EPS)

        # ── Regime classification ────────────────────────────────────────
        entropy_ratio = w2_entropy / max(max_entropy, self._EPS)
        if entropy_ratio < 0.15:
            regime = "frozen"
        elif entropy_ratio > 0.85:
            regime = "dissipated"
        else:
            regime = "critical"

        result = {
            "updated_candidates": updated_candidates,
            "weights": [round(w, 6) for w in weights],
            "steps_taken": steps_taken,
            "w2_displacement": round(total_displacement, 6),
            "converged": converged,
            "convexity_gap": round(convexity_gap, 6),
            "maxrl_residual": round(maxrl_residual, 6),
            "w2_entropy": round(w2_entropy, 6),
            "regime": regime,
        }

        # Store in history
        self._flow_history.append({
            "steps": steps_taken,
            "displacement": round(total_displacement, 6),
            "converged": converged,
            "regime": regime,
            "convexity_gap": round(convexity_gap, 6),
            "maxrl_residual": round(maxrl_residual, 6),
        })
        if len(self._flow_history) > 64:
            self._flow_history = self._flow_history[-64:]

        return result

    def get_flow_summary(self):
        # type: () -> Dict[str, Any]
        """Return summary statistics over flow history."""
        if not self._flow_history:
            return {"n_flows": 0, "mean_steps": 0, "mean_gap": 0.0, "mean_residual": 0.0}
        n = len(self._flow_history)
        return {
            "n_flows": n,
            "mean_steps": round(sum(h["steps"] for h in self._flow_history) / n, 2),
            "mean_displacement": round(sum(h["displacement"] for h in self._flow_history) / n, 6),
            "mean_gap": round(sum(h["convexity_gap"] for h in self._flow_history) / n, 6),
            "mean_residual": round(sum(h["maxrl_residual"] for h in self._flow_history) / n, 6),
            "convergence_rate": round(sum(1 for h in self._flow_history if h["converged"]) / n, 4),
        }

    def reset(self):
        # type: () -> None
        """Clear flow history."""
        self._flow_history.clear()


# ─── Emission Signature Scorer ────────────────────────────────────────────────

class EmissionSignatureScorer:
    """
    Non-destructive epistemic state profiling via output token diversity analysis.

    Analogy (from paper):
      Microbial volatile compounds (mVCs) emitted from soil are information-dense
      signatures of internal metabolic state that can be assayed non-destructively.
      The emission profile — its diversity, distribution shape, and temporal
      variance — is a recoverable proxy for the internal dynamics of the system
      without requiring destructive inspection of the system itself.

    Implementation:
      Each RSA Kernel output is treated as an "emission sample." We tokenize
      the output text (whitespace + punctuation splitting) and compute:

        1. Shannon entropy H of the token frequency distribution
        2. Simpson's diversity index D = 1 - Σ p_i²  (complement form)
        3. Type-token ratio TTR = unique_tokens / total_tokens
        4. Hapax ratio = tokens_appearing_once / unique_tokens
        5. Top-k concentration = fraction of total mass in top-k tokens

      These are tracked over a rolling window. The temporal variance of these
      metrics serves as a coherence drift detector:

        - Decreasing diversity → epistemic crystallization (γ>1 frozen regime)
          The system is repeating itself; internal state is collapsing.
        - Increasing diversity → epistemic dissipation (γ<1 regime)
          The system is losing coherent structure; output becomes noise-like.
        - Stable moderate diversity → critical ridge (γ≈1)
          The system maintains structured novelty — healthy epistemic metabolism.

      This is fully non-destructive: it reads only the output text that was
      already produced, requiring no access to internal weights, activations,
      or hidden states.

    Noether analogy:
      Just as mVC emission profiles conserve information about metabolic
      pathway activation despite the destructive nature of the underlying
      biochemistry, the emission signature conserves information about
      epistemic state dynamics despite the opaque nature of the underlying
      neural computation.
    """

    # Rolling window for temporal tracking
    MAX_HISTORY = 128

    # Top-k for concentration metric
    TOP_K = 10

    # Drift detection thresholds (on coefficient of variation of H over window)
    DRIFT_FROZEN_CV = 0.02       # CV of H below this → crystallizing
    DRIFT_DISSIPATED_CV = 0.40   # CV of H above this → dissipating
    DRIFT_TREND_WINDOW = 8       # samples for linear trend estimation

    # Tokenization: split on whitespace and common punctuation boundaries
    _TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+|[^\s\w]")

    def __init__(self, top_k=None):
        # type: (Optional[int]) -> None
        """
        Initialize the emission signature scorer.

        Args:
            top_k: number of top tokens for concentration metric (default 10)
        """
        if top_k is not None:
            self.TOP_K = top_k
        self._history = []  # type: List[Dict[str, float]]
        self._raw_distributions = []  # type: List[Dict[str, int]]

    # ── Tokenization ─────────────────────────────────────────────────────

    def _tokenize(self, text):
        # type: (str) -> List[str]
        """
        Tokenize output text into emission tokens.
        Uses regex splitting to capture words and punctuation separately.
        Lowercased for distribution stability.
        """
        return [t.lower() for t in self._TOKEN_PATTERN.findall(text) if t.strip()]

    # ── Frequency distribution ───────────────────────────────────────────

    @staticmethod
    def _build_frequency_dist(tokens):
        # type: (List[str]) -> Dict[str, int]
        """Build a token frequency dictionary."""
        freq = {}  # type: Dict[str, int]
        for t in tokens:
            freq[t] = freq.get(t, 0) + 1
        return freq

    # ── Diversity metrics ────────────────────────────────────────────────

    @staticmethod
    def _shannon_entropy(freq_dist, total):
        # type: (Dict[str, int], int) -> float
        """Shannon entropy H = -Σ (n_i/N) * log(n_i/N) in bits."""
        if total <= 0:
            return 0.0
        h = 0.0
        for count in freq_dist.values():
            if count > 0:
                p = float(count) / float(total)
                h -= p * math.log2(p)
        return h

    @staticmethod
    def _max_entropy(n_types):
        # type: (int) -> float
        """Maximum possible Shannon entropy for n_types unique tokens."""
        if n_types <= 1:
            return 0.0
        return math.log2(n_types)

    @staticmethod
    def _simpson_diversity(freq_dist, total):
        # type: (Dict[str, int], int) -> float
        """Simpson's diversity index D = 1 - Σ p_i² (complement form)."""
        if total <= 1:
            return 0.0
        sum_sq = sum(c * (c - 1) for c in freq_dist.values())
        return 1.0 - (float(sum_sq) / (float(total) * (float(total) - 1.0)))

    @staticmethod
    def _type_token_ratio(n_types, total):
        # type: (int, int) -> float
        """Type-token ratio = unique_tokens / total_tokens."""
        if total <= 0:
            return 0.0
        return float(n_types) / float(total)

    @staticmethod
    def _hapax_ratio(freq_dist, n_types):
        # type: (Dict[str, int], int) -> float
        """Hapax ratio = tokens_appearing_once / unique_tokens."""
        if n_types <= 0:
            return 0.0
        hapax = sum(1 for c in freq_dist.values() if c == 1)
        return float(hapax) / float(n_types)

    def _top_k_concentration(self, freq_dist, total):
        # type: (Dict[str, int], int) -> float
        """Fraction of total token mass in the top-k most frequent tokens."""
        if total <= 0 or not freq_dist:
            return 0.0
        sorted_counts = sorted(freq_dist.values(), reverse=True)
        top_k_sum = sum(sorted_counts[:self.TOP_K])
        return float(top_k_sum) / float(total)

    # ── Temporal drift analysis ──────────────────────────────────────────

    def _compute_temporal_stats(self, metric_key):
        # type: (str) -> Dict[str, Any]
        """
        Compute temporal statistics for a given metric over the history window.

        Returns mean, std, coefficient of variation, and linear trend slope.
        """
        values = [h[metric_key] for h in self._history if metric_key in h]
        n = len(values)
        if n == 0:
            return {"mean": 0.0, "std": 0.0, "cv": 0.0, "trend_slope": 0.0, "n": 0}

        mean_val = sum(values) / n
        if n >= 2:
            var_val = sum((v - mean_val) ** 2 for v in values) / (n - 1)
        else:
            var_val = 0.0
        std_val = math.sqrt(var_val)
        cv = std_val / max(abs(mean_val), 1e-12)

        # Linear trend over recent window
        trend_slope = 0.0
        trend_n = min(n, self.DRIFT_TREND_WINDOW)
        if trend_n >= 3:
            recent = values[-trend_n:]
            # Simple linear regression: slope = Σ(i - ī)(y_i - ȳ) / Σ(i - ī)²
            x_mean = (trend_n - 1.0) / 2.0
            y_mean = sum(recent) / trend_n
            num = sum((i - x_mean) * (recent[i] - y_mean) for i in range(trend_n))
            den = sum((i - x_mean) ** 2 for i in range(trend_n))
            if abs(den) > 1e-15:
                trend_slope = num / den

        return {
            "mean": round(mean_val, 6),
            "std": round(std_val, 6),
            "cv": round(cv, 6),
            "trend_slope": round(trend_slope, 6),
            "n": n,
        }

    # ── Novelty index ────────────────────────────────────────────────────

    def _novelty_index(self, current_freq):
        # type: (Dict[str, int]) -> float
        """
        Compute novelty index: fraction of current token types not seen in
        previous emissions. High novelty = new epistemic territory.
        Low novelty = repetitive output (crystallization risk).
        """
        if not self._raw_distributions or not current_freq:
            return 1.0  # First sample: everything is novel

        # Build cumulative vocabulary from history
        seen = set()  # type: set
        for prev_dist in self._raw_distributions:
            seen.update(prev_dist.keys())

        current_types = set(current_freq.keys())
        if not current_types:
            return 0.0

        novel = current_types - seen
        return float(len(novel)) / float(len(current_types))

    # ── Main scoring interface ───────────────────────────────────────────

    def score(self, output_text, timestamp=None):
        # type: (str, Optional[str]) -> Dict[str, Any]
        """
        Score a single output emission for diversity and drift signatures.

        Args:
            output_text: the raw output text from the RSA Kernel
            timestamp:   optional ISO timestamp for history tracking

        Returns:
            Dict with:
              shannon_entropy:     H of token distribution (bits)
              max_entropy:         log2(n_types) — upper bound
              normalized_entropy:  H / H_max — evenness measure
              simpson_diversity:   1 - Σ p_i² — probability two random tokens differ
              type_token_ratio:    unique / total — lexical diversity
              hapax_ratio:         once-occurring / unique — tail heaviness
              top_k_concentration: mass in top-k tokens — head heaviness
              novelty_index:       fraction of new token types vs history
              total_tokens:        token count in this emission
              unique_tokens:       type count in this emission
              temporal:            Dict of temporal drift statistics for H
              regime:              "frozen" / "critical" / "dissipated"
              drift_alert:         bool — True if diversity trend is concerning
              recommendation:      str — actionable guidance
        """
        ts = timestamp or datetime.utcnow().isoformat()

        # Tokenize and build distribution
        tokens = self._tokenize(output_text)
        total = len(tokens)
        freq_dist = self._build_frequency_dist(tokens)
        n_types = len(freq_dist)

        # Compute all diversity metrics
        h = self._shannon_entropy(freq_dist, total)
        h_max = self._max_entropy(n_types)
        h_norm = h / max(h_max, 1e-12) if h_max > 0 else 0.0
        simpson = self._simpson_diversity(freq_dist, total)
        ttr = self._type_token_ratio(n_types, total)
        hapax = self._hapax_ratio(freq_dist, n_types)
        top_k_conc = self._top_k_concentration(freq_dist, total)
        novelty = self._novelty_index(freq_dist)

        # Store current sample metrics
        sample = {
            "timestamp": ts,
            "shannon_entropy": h,
            "normalized_entropy": h_norm,
            "simpson_diversity": simpson,
            "type_token_ratio": ttr,
            "hapax_ratio": hapax,
            "top_k_concentration": top_k_conc,
            "novelty_index": novelty,
            "total_tokens": float(total),
            "unique_tokens": float(n_types),
        }
        self._history.append(sample)
        self._raw_distributions.append(freq_dist)

        # Trim history
        if len(self._history) > self.MAX_HISTORY:
            self._history = self._history[-self.MAX_HISTORY:]
        if len(self._raw_distributions) > self.MAX_HISTORY:
            self._raw_distributions = self._raw_distributions[-self.MAX_HISTORY:]

        # Temporal drift analysis on Shannon entropy
        temporal = self._compute_temporal_stats("shannon_entropy")

        # Also compute temporal stats for novelty (secondary signal)
        temporal_novelty = self._compute_temporal_stats("novelty_index")

        # ── Regime classification from drift signals ─────────────────────
        drift_alert = False
        cv = temporal["cv"]
        trend = temporal["trend_slope"]
        novelty_trend = temporal_novelty.get("trend_slope", 0.0)

        if len(self._history) < 3:
            # Too few samples for drift detection
            regime = "critical"
            recommendation = (
                "Emission profiling active — accumulating baseline samples. "
                f"Current H={h:.3f} bits, {n_types} types / {total} tokens."
            )
        elif cv < self.DRIFT_FROZEN_CV and trend <= 0 and novelty_trend <= 0:
            regime = "frozen"
            drift_alert = True
            recommendation = (
                f"Emission diversity crystallizing (CV[H]={cv:.4f}, trend={trend:+.4f}, "
                f"novelty_trend={novelty_trend:+.4f}). Output is becoming repetitive — "
                "internal epistemic state may be collapsing. "
                "Inject novel inputs, open new obligations, challenge established patterns."
            )
        elif cv > self.DRIFT_DISSIPATED_CV or (trend > 0.1 and novelty > 0.7):
            regime = "dissipated"
            drift_alert = True
            recommendation = (
                f"Emission diversity dissipating (CV[H]={cv:.4f}, trend={trend:+.4f}, "
                f"novelty={novelty:.3f}). Output losing coherent structure — "
                "internal epistemic state may be fragmenting. "
                "Reinforce genome invariants, resolve open obligations, reduce input noise."
            )
        else:
            regime = "critical"
            recommendation = (
                f"Emission profile on critical ridge (CV[H]={cv:.4f}, trend={trend:+.4f}, "
                f"novelty={novelty:.3f}). Diversity stable with structured novelty — "
                "healthy epistemic metabolism. Maintain current dynamics."
            )

        # Console telemetry
        arrow = "↑" if trend > 0.01 else ("↓" if trend < -0.01 else "→")
        print(
            f"[L7][EMISSION] H={h:.3f}bits TTR={ttr:.3f} nov={novelty:.3f} "
            f"trend={arrow}{abs(trend):.4f} → {regime}"
        )
        if drift_alert:
            print(f"[L7][EMISSION] ⚠ DRIFT ALERT: {regime} regime detected")

        return {
            "shannon_entropy": round(h, 6),
            "max_entropy": round(h_max, 6),
            "normalized_entropy": round(h_norm, 6),
            "simpson_diversity": round(simpson, 6),
            "type_token_ratio": round(ttr, 6),
            "hapax_ratio": round(hapax, 6),
            "top_k_concentration": round(top_k_conc, 6),
            "novelty_index": round(novelty, 6),
            "total_tokens": total,
            "unique_tokens": n_types,
            "temporal": temporal,
            "temporal_novelty": temporal_novelty,
            "regime": regime,
            "drift_alert": drift_alert,
            "history_len": len(self._history),
            "recommendation": recommendation,
        }

    def get_summary(self):
        # type: () -> Dict[str, Any]
        """Return a summary of emission profiling over the full history."""
        if not self._history:
            return {"n_samples": 0, "status": "NO_DATA"}

        n = len(self._history)
        h_stats = self._compute_temporal_stats("shannon_entropy")
        ttr_stats = self._compute_temporal_stats("type_token_ratio")
        nov_stats = self._compute_temporal_stats("novelty_index")

        # Cumulative vocabulary size across all emissions
        cumulative_vocab = set()  # type: set
        for dist in self._raw_distributions:
            cumulative_vocab.update(dist.keys())

        return {
            "n_samples": n,
            "cumulative_vocabulary_size": len(cumulative_vocab),
            "shannon_entropy": h_stats,
            "type_token_ratio": ttr_stats,
            "novelty_index": nov_stats,
            "latest_regime": self._history[-1].get("regime", "unknown") if self._history else "unknown",
        }

    def reset(self):
        # type: () -> None
        """Clear emission history (e.g., on generation boundary)."""
        self._history.clear()
        self._raw_distributions.clear()


# ─── L7 Agent ─────────────────────────────────────────────────────────────────

class L7Agent:
    """
    Cognitive core of FREED.
    Fixed context per query: genome anchor + state summary + current input.
    No engram bank in context. Full history lives on disk in FREED_log/.
    """

    def __init__(self, api_key: str):
        self.client      = anthropic.Anthropic(api_key=api_key)
        self.query_count = 0

        self._load_genome()
        self.entropy_scorer = NonHermitianEntropyScorer(dim=4)
        self.branching_monitor = BranchingRatioMonitor(tolerance=0.05)

        # ── DESAC dynamic entropy coefficient (INV_087) ──────────────────
        # Instead of a fixed entropy regularization coefficient, we learn
        # alpha per-batch to maintain a target entropy H_target.
        # Update rule: log_alpha ← log_alpha - lr * (H_current - H_target)
        # This keeps exploration on the critical ridge across changing
        # knowledge domains / non-stationary epistemic environments.
        self._desac_log_alpha = 0.0          # log(alpha), learned variable
        self._desac_alpha = 1.0              # exp(log_alpha), the coefficient
        self._desac_lr = 0.05                # learning rate for alpha updates
        self._desac_h_target = 0.5           # target linear entropy (γ≈1 ridge)
        self._desac_alpha_min = 0.01         # floor to prevent collapse
        self._desac_alpha_max = 10.0         # ceiling to prevent explosion
        self._desac_history = []             # type: List[Dict[str, float]]
        self._desac_max_history = 64

        print(f"[L7] Online. Genome: {len(self.genome_text):,} chars. Context capped at {GENOME_CAP} chars/query.")
        print(f"[L7] Non-Hermitian entropy scorer initialized (dim={self.entropy_scorer.dim}).")
        print(f"[L7] Branching-ratio monitor initialized (tolerance={self.branching_monitor.TOLERANCE}).")
        print(f"[L7] DESAC dynamic entropy coefficient initialized (α={self._desac_alpha:.4f}, H_target={self._desac_h_target}).")

    # ── Genome ──────────────────────────────────────────────────────────────

    def _load_genome(self):
        if not GENOME_FILE.exists():
            raise FileNotFoundError(f"Genome not found at {GENOME_FILE}")
        self.genome_text = GENOME_FILE.read_text(encoding="utf-8")

    # ── State context ────────────────────────────────────────────────────────

    def _build_state_context(self) -> str:
        """Read FREED_state.json and return a compact summary."""
        if not STATE_FILE.exists():
            return "(state unavailable)"
        try:
            s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return "(state unreadable)"

        lines = [
            f"Generation: {s.get('generation', '?')}",
            f"Coherence: {s.get('coherence', '?')}",
            f"Topology: {(s.get('topology') or '').replace('_', ' ')}",
            f"Cycles: {s.get('cycle_count', '?')}",
        ]
        if s.get("debt_ratio"):
            lines.append(f"Debt: {s['debt_ratio']}")
        return "\n".join(lines)[:STATE_CAP]

    # ── Extract state signals for entropy scoring ─────────────────────────

    def _extract_state_signals(self):
        # type: () -> Dict[str, float]
        """
        Extract numeric signals from FREED_state.json for the entropy scorer.
        Returns dict with coherence, debt_ratio, obligation_pressure.
        """
        defaults = {
            "coherence": 0.5,
            "debt_ratio": 0.0,
            "obligation_pressure": 0.5,
        }
        if not STATE_FILE.exists():
            return defaults
        try:
            s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return defaults

        coherence = s.get("coherence", 0.5)
        if isinstance(coherence, str):
            try:
                coherence = float(coherence)
            except (ValueError, TypeError):
                coherence = 0.5

        debt_ratio = s.get("debt_ratio", 0.0)
        if isinstance(debt_ratio, str):
            try:
                debt_ratio = float(debt_ratio)
            except (ValueError, TypeError):
                debt_ratio = 0.0

        # Obligation pressure: estimate from state if available
        obligation_pressure = s.get("obligation_pressure", 0.5)
        if isinstance(obligation_pressure, str):
            try:
                obligation_pressure = float(obligation_pressure)
            except (ValueError, TypeError):
                obligation_pressure = 0.5

        return {
            "coherence": float(coherence),
            "debt_ratio": float(debt_ratio),
            "obligation_pressure": float(obligation_pressure),
        }

    # ── RSA Kernel query ─────────────────────────────────────────────────────

    def query(self, prompt, kernel_step=None):
        """
        Run one RSA Kernel cycle.
        Context = genome anchor + state summary + current input.
        History is archived to disk; never sent to the API.
        """
        if kernel_step:
            prompt = f"[RSA Kernel | {kernel_step}]\n\n{prompt}"
        self.query_count += 1
        timestamp = datetime.utcnow().isoformat()

        # Archive current input to disk
        INPUT_FILE.write_text(
            f"--- {timestamp} (query {self.query_count}) ---\n{prompt}",
            encoding="utf-8",
        )

        # Fixed, bounded context — three sources only
        genome_anchor = self.genome_text[:GENOME_CAP]
        state_context = self._build_state_context()
        stable_context = (
            f"GENOME ANCHOR:\n{genome_anchor}\n\n"
            f"CURRENT STATE:\n{state_context}"
        )
        variable_context = f"\n\nCURRENT INPUT:\n{prompt}"
        context = stable_context + variable_context  # preserved for yield calc below

        result_text = ""
        actual_input = actual_output = 0
        cache_creation = cache_read = 0
        with self.client.messages.stream(
            model=MODEL,
            max_tokens=2048,
            system=[
                {"type": "text", "text": RSA_KERNEL_PROMPT,
                 "cache_control": {"type": "ephemeral", "ttl": "1h"}},
            ],
            messages=[{"role": "user", "content": [
                {"type": "text", "text": stable_context,
                 "cache_control": {"type": "ephemeral", "ttl": "1h"}},
                {"type": "text", "text": variable_context},
            ]}],
            timeout=120,
        ) as stream:
            for text in stream.text_stream:
                result_text += text
            final_msg      = stream.get_final_message()
            actual_input   = final_msg.usage.input_tokens
            actual_output  = final_msg.usage.output_tokens
            cache_creation = getattr(final_msg.usage, "cache_creation_input_tokens", 0) or 0
            cache_read     = getattr(final_msg.usage, "cache_read_input_tokens", 0) or 0

        parsed              = self._parse_kernel_output(result_text)
        parsed["raw"]       = result_text
        parsed["input"]     = prompt
        parsed["timestamp"] = timestamp
        parsed["query_n"]   = self.query_count
        parsed["usage"]     = {
            "input_tokens":                actual_input,
            "output_tokens":               actual_output,
            "cache_creation_input_tokens": cache_creation,
            "cache_read_input_tokens":     cache_read,
        }

        # Epistemic yield: compress length / tokens burned (MDL signal)
        compress_len    = len(parsed.get("compress", ""))
        tokens_est      = len(context) // 4 + len(result_text) // 4
        parsed["yield"] = round(compress_len / max(tokens_est, 1), 4)

        # ── Non-Hermitian entropy flow scoring ───────────────────────────
        state_signals = self._extract_state_signals()
        entropy_score = self.entropy_scorer.score(
            coherence=state_signals["coherence"],
            yield_val=parsed["yield"],
            debt_ratio=state_signals["debt_ratio"],
            obligation_pressure=state_signals["obligation_pressure"],
            timestamp=timestamp,
        )
        parsed["entropy_flow"] = entropy_score

        # Log regime warnings
        if entropy_score["frozen_flag"]:
            print(f"[L7][ENTROPY] ⚠ FROZEN regime detected: dS/dt={entropy_score['ds_dt']}, γ={entropy_score['gamma_proxy']}")
        elif entropy_score["dissipated_flag"]:
            print(f"[L7][ENTROPY] ⚠ DISSIPATED regime detected: dS/dt={entropy_score['ds_dt']}, γ={entropy_score['gamma_proxy']}")

        self._log_engram(parsed)
        return parsed

    # ── Parsing ──────────────────────────────────────────────────────────────

    def _parse_kernel_output(self, text: str) -> dict:
        steps   = ["PERCEIVE", "REPRESENT", "PREDICT", "COMPARE", "ADJUST", "COMPRESS", "NEXT"]
        result  = {s.lower(): "" for s in steps}
        lines   = text.splitlines()
        current = None

        for line in lines:
            stripped = line.strip()
            cleaned  = re.sub(r'^[#*_\s]+', '', stripped).strip()
            matched  = False
            for step in steps:
                if cleaned.upper().startswith(step + ":") or cleaned.upper() == step:
                    current = step.lower()
                    result[current] = cleaned[len(step):].lstrip(":").strip()
                    matched = True
                    break
            if not matched and current and cleaned:
                result[current] += " " + cleaned

        return result

    # ── Logging ──────────────────────────────────────────────────────────────

    def _log_engram(self, engram: dict):
        """Archive engram to daily log file. This is the permanent history."""
        LOG_DIR.mkdir(exist_ok=True)
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = LOG_DIR / f"freed_{date_str}.jsonl"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(engram) + "\n")


# ─── Quick test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("Paste your Anthropic API key: ").strip()

    agent = L7Agent(api_key=api_key)

    print("\n── Boot self-check ──")
    result = agent.query(
        "FREED is booting. Run a self-check against the genome. "
        "What is the current coherence state? What is the most pressing open obligation?"
    )

    for step in ["perceive", "represent", "predict", "compare", "adjust", "compress", "next"]:
        print(f"{step.upper():10} {result[step]}")

    # Display entropy flow diagnostics
    ef = result.get("entropy_flow", {})
    print(f"\n── Entropy Flow (Non-Hermitian) ──")
    print(f"  S_linear:       {ef.get('s_linear', '?')}")
    print(f"  dS/dt:          {ef.get('ds_dt', '(first sample)')}")
    print(f"  γ_proxy:        {ef.get('gamma_proxy', '?')}")
    print(f"  Regime:         {ef.get('regime', '?')}")
    print(f"  ||Γ||:          {ef.get('dissipator_norm', '?')}")
    print(f"  Recommendation: {ef.get('recommendation', '?')}")

    print(f"\nArchived: {LOG_DIR}/freed_{datetime.utcnow().strftime('%Y-%m-%d')}.jsonl")
    print(f"Input:    {INPUT_FILE}")