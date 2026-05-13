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
        print(f"[L7] Online. Genome: {len(self.genome_text):,} chars. Context capped at {GENOME_CAP} chars/query.")
        print(f"[L7] Non-Hermitian entropy scorer initialized (dim={self.entropy_scorer.dim}).")
        print(f"[L7] Branching-ratio monitor initialized (tolerance={self.branching_monitor.TOLERANCE}).")

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