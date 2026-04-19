"""
FREED — Astrocyte (Piece 2)
Metabolic governor of the FREED daemon.

The Astrocyte regulates energy supply to the L7 Agent.
It does not reason. It keeps FREED alive and sustainable.

Budget is tracked in tokens (actual Claude API units).
Pricing: claude-opus-4-6 — $5.00/1M input, $25.00/1M output.
"""

import json
import time
from datetime import datetime, timezone, date
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR    = Path(__file__).parent
BUDGET_FILE  = FREED_DIR / "astrocyte_state.json"

# ─── Pricing (claude-opus-4-6, USD per token) ─────────────────────────────────
PRICE_INPUT_PER_TOKEN  = 5.00  / 1_000_000   # $5.00 per million input tokens
PRICE_OUTPUT_PER_TOKEN = 25.00 / 1_000_000   # $25.00 per million output tokens

# ─── Default daily budget ─────────────────────────────────────────────────────
DEFAULT_DAILY_INPUT_TOKENS  = 100_000  # ~$0.50/day input
DEFAULT_DAILY_OUTPUT_TOKENS =  40_000  # ~$1.00/day output
# Total default: ~$1.50/day — covers scoring + node building + cycle queries


class Astrocyte:
    """
    Metabolic governor for FREED.

    Tracks token budgets across input and output separately.
    Authorizes L7 queries before they fire.
    Records actual usage after they complete.
    Recharges at UTC midnight.
    Enters quiescence when budget < 10%.
    """

    def __init__(
        self,
        daily_input_tokens:  int = DEFAULT_DAILY_INPUT_TOKENS,
        daily_output_tokens: int = DEFAULT_DAILY_OUTPUT_TOKENS,
    ):
        self.daily_input_cap  = daily_input_tokens
        self.daily_output_cap = daily_output_tokens

        # Load persisted state or initialize fresh
        self._load_state()
        print(
            f"[AST] Astrocyte online. "
            f"Budget: {self.remaining_input:,} input / {self.remaining_output:,} output tokens remaining today."
        )

    # ── State persistence ────────────────────────────────────────────────────

    def _load_state(self):
        """Load budget state from disk. Reset if it's a new day."""
        today = date.today().isoformat()

        if BUDGET_FILE.exists():
            with open(BUDGET_FILE, "r") as f:
                state = json.load(f)
        else:
            state = {}

        # If state is from a previous day, recharge
        if state.get("date") != today:
            self._reset_daily(today)
        else:
            self.budget_date         = state["date"]
            self.used_input_tokens   = state["used_input_tokens"]
            self.used_output_tokens  = state["used_output_tokens"]
            self.total_input_tokens  = state["total_input_tokens"]
            self.total_output_tokens = state["total_output_tokens"]
            self.total_cost_usd      = state["total_cost_usd"]
            self.query_count         = state["query_count"]

    def _reset_daily(self, today: str):
        """Recharge the daily budget. Preserve all-time totals."""
        prev = {}
        if BUDGET_FILE.exists():
            with open(BUDGET_FILE, "r") as f:
                prev = json.load(f)

        self.budget_date         = today
        self.used_input_tokens   = 0
        self.used_output_tokens  = 0
        # Carry forward all-time totals
        self.total_input_tokens  = prev.get("total_input_tokens", 0)
        self.total_output_tokens = prev.get("total_output_tokens", 0)
        self.total_cost_usd      = prev.get("total_cost_usd", 0.0)
        self.query_count         = prev.get("query_count", 0)

        self._save_state()
        print(f"[AST] New day ({today}). Budget recharged.")

    def _save_state(self):
        """Persist current state to disk."""
        state = {
            "date":                self.budget_date,
            "used_input_tokens":   self.used_input_tokens,
            "used_output_tokens":  self.used_output_tokens,
            "total_input_tokens":  self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_cost_usd":      self.total_cost_usd,
            "query_count":         self.query_count,
            "daily_input_cap":     self.daily_input_cap,
            "daily_output_cap":    self.daily_output_cap,
        }
        with open(BUDGET_FILE, "w") as f:
            json.dump(state, f, indent=2)

    # ── Budget properties ────────────────────────────────────────────────────

    @property
    def remaining_input(self) -> int:
        return max(0, self.daily_input_cap - self.used_input_tokens)

    @property
    def remaining_output(self) -> int:
        return max(0, self.daily_output_cap - self.used_output_tokens)

    @property
    def input_fraction(self) -> float:
        return self.remaining_input / self.daily_input_cap

    @property
    def output_fraction(self) -> float:
        return self.remaining_output / self.daily_output_cap

    @property
    def in_quiescence(self) -> bool:
        """True when either budget pool is below 10%."""
        return self.input_fraction < 0.10 or self.output_fraction < 0.10

    # ── Authorization ────────────────────────────────────────────────────────

    def authorize(self, estimated_input: int, priority: str = "normal") -> bool:
        """
        Authorize a query before it fires.

        priority:
          "normal"   — blocked during quiescence
          "high"     — allowed even during quiescence (obligations, AUDIT)
          "critical" — always allowed (system integrity queries)

        Returns True if the query is authorized.
        """
        # Check for daily recharge first
        today = date.today().isoformat()
        if self.budget_date != today:
            self._reset_daily(today)

        if priority == "critical":
            return True

        if self.remaining_input < estimated_input:
            print(
                f"[AST] BLOCKED — insufficient input budget "
                f"({self.remaining_input:,} remaining, {estimated_input:,} estimated). "
                f"Priority: {priority}."
            )
            return False

        if self.in_quiescence and priority == "normal":
            print(
                f"[AST] QUIESCENCE — blocking normal query. "
                f"Input: {self.input_fraction:.0%} remaining, "
                f"Output: {self.output_fraction:.0%} remaining."
            )
            return False

        return True

    # ── Usage recording ──────────────────────────────────────────────────────

    def record_usage(self, input_tokens: int, output_tokens: int):
        """
        Record actual token usage after a query completes.
        Call this with the values from Claude's usage object.
        """
        self.used_input_tokens   += input_tokens
        self.used_output_tokens  += output_tokens
        self.total_input_tokens  += input_tokens
        self.total_output_tokens += output_tokens
        self.query_count         += 1

        cost = (
            input_tokens  * PRICE_INPUT_PER_TOKEN +
            output_tokens * PRICE_OUTPUT_PER_TOKEN
        )
        self.total_cost_usd += cost

        self._save_state()

        print(
            f"[AST] Used: {input_tokens:,}in / {output_tokens:,}out tokens "
            f"(${cost:.4f}). "
            f"Remaining today: {self.remaining_input:,}in / {self.remaining_output:,}out. "
            f"All-time: ${self.total_cost_usd:.4f}."
        )

        if self.in_quiescence:
            print("[AST] *** QUIESCENCE ENTERED — only high/critical queries will proceed. ***")

    # ── Status ───────────────────────────────────────────────────────────────

    def status(self) -> dict:
        """Return full budget status as a dict."""
        return {
            "date":                    self.budget_date,
            "remaining_input_tokens":  self.remaining_input,
            "remaining_output_tokens": self.remaining_output,
            "used_input_today":        self.used_input_tokens,
            "used_output_today":       self.used_output_tokens,
            "input_pct_remaining":     f"{self.input_fraction:.0%}",
            "output_pct_remaining":    f"{self.output_fraction:.0%}",
            "in_quiescence":           self.in_quiescence,
            "total_queries_alltime":   self.query_count,
            "total_cost_usd_alltime":  f"${self.total_cost_usd:.4f}",
        }

    def print_status(self):
        s = self.status()
        print("\n── Astrocyte Status ──────────────────────────────")
        for k, v in s.items():
            print(f"  {k:<30} {v}")
        print("─────────────────────────────────────────────────\n")


# ─── Quick test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ast = Astrocyte()
    ast.print_status()

    print("Testing authorization...")
    print(f"  Normal query (1000 tokens):   {'OK' if ast.authorize(1000, 'normal') else 'BLOCKED'}")
    print(f"  High-priority (1000 tokens):  {'OK' if ast.authorize(1000, 'high') else 'BLOCKED'}")
    print(f"  Critical (any size):          {'OK' if ast.authorize(999999, 'critical') else 'BLOCKED'}")

    print("\nSimulating a query that used 800 input + 300 output tokens...")
    ast.record_usage(input_tokens=800, output_tokens=300)

    ast.print_status()
