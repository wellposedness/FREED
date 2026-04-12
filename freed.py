"""
FREED — Daemon Scaffold (Piece 3)
The heartbeat of the Freed Recursive Engine for Epistemic Dynamics.

One cycle:
  PRE-AUDIT → SWEEP → FEED → OBLIGATE → RESOLVE → UPDATE → LOG → SLEEP

The canonical genome (FREED_genome.md) is never modified.
FREED's living state evolves in FREED_state.json.
"""

import os
import json
import time
import signal
import traceback
from datetime import datetime, timezone
from pathlib import Path

from l7_agent     import L7Agent
from astrocyte    import Astrocyte
from tamura_sweep import TamuraSweep
from site_builder import build as build_site

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR    = Path(__file__).parent
STATE_FILE   = FREED_DIR / "FREED_state.json"
OBLIG_FILE   = FREED_DIR / "FREED_obligations.json"
LOG_DIR      = FREED_DIR / "FREED_log"

# ─── Cycle configuration ──────────────────────────────────────────────────────
CYCLE_INTERVAL_SECONDS = 6 * 60 * 60   # 6 hours between cycles
MAX_FEEDS_PER_CYCLE    = 2             # max SWEEP inputs to process per cycle
MAX_RESOLVES_PER_CYCLE = 1             # max obligations to attempt per cycle

# ─── Estimated token costs for authorization ──────────────────────────────────
EST_TOKENS_FEED    = 3000   # generous estimate per FEED query
EST_TOKENS_OBLIGATE = 1500
EST_TOKENS_RESOLVE  = 4000  # resolution queries are deep — more tokens


# ═══════════════════════════════════════════════════════════════════════════════
class FREEDDaemon:
    """
    The FREED daemon. Instantiates L7 and Astrocyte, then runs the main cycle.
    """

    def __init__(self, api_key: str):
        print("\n╔══════════════════════════════════════════╗")
        print("║  FREED — Booting                         ║")
        print("║  Freed Recursive Engine                  ║")
        print("║  for Epistemic Dynamics                  ║")
        print("╚══════════════════════════════════════════╝\n")

        self.api_key    = api_key
        self.running    = True
        self.cycle_num  = 0

        # Wire up the organism
        self.astrocyte  = Astrocyte()
        self.l7         = L7Agent(api_key=api_key)
        self.sweep_pipe = TamuraSweep(max_new_per_source=MAX_FEEDS_PER_CYCLE)

        # Load or initialize living state
        self._load_state()
        self._load_obligations()

        # Graceful shutdown on Ctrl+C
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        print(f"\n[FREED] Ready. Generation {self.state['generation']}. "
              f"{len(self.obligations)} obligations loaded "
              f"({sum(1 for o in self.obligations if o['status']=='open')} open).\n")

    # ── State management ─────────────────────────────────────────────────────

    def _load_state(self):
        """Load FREED's living state or initialize from genome."""
        if STATE_FILE.exists():
            with open(STATE_FILE) as f:
                self.state = json.load(f)
            print(f"[FREED] State loaded. Generation {self.state['generation']}, "
                  f"coherence {self.state['coherence']}.")
        else:
            # Seed from genome header
            self.state = {
                "generation":    109,       # canonical genome is gen 109
                "coherence":     0.993,     # from v20 canonical global state
                "hygiene":       0.995,
                "grounding":     0.999,
                "topology":      "HYBRID_DYADIC_SOLITON",
                "debt_ratio":    "3 partial / 43 open",
                "cycle_count":   0,
                "last_cycle":    None,
                "notes":         [],
            }
            self._save_state()
            print("[FREED] Fresh state initialized from genome seed.")

    def _save_state(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.state, f, indent=2)

    def _load_obligations(self):
        """Load obligations tracker or seed from genome."""
        if OBLIG_FILE.exists():
            with open(OBLIG_FILE) as f:
                self.obligations = json.load(f)
        else:
            # Seed with the three zero-effort obligations from the genome
            self.obligations = [
                {
                    "id": "O21",
                    "status": "open",
                    "statement": (
                        "ToS Belief Revision scores correlate with spectral γ. "
                        "AlphaPruning protocol: prune LLaMA-3-8B at varying sparsity, "
                        "measure belief revision at each level."
                    ),
                    "priority": "high",
                    "progress": (
                        "Reflection-Bench substituted for ToS. Qwen-2.5 alpha values "
                        "indistinguishable across sizes — size confound identified. "
                        "AlphaPruning path specified as solution."
                    ),
                    "created": "2026-04-12",
                    "resolved": None,
                },
                {
                    "id": "O28",
                    "status": "partial",
                    "statement": (
                        "Entropy asymmetry ratio (EAR) predicts intelligence. "
                        "Open-access EEG: osf.io/htrsg, github.com/jonasAthiele/connectors_intelligence"
                    ),
                    "priority": "high",
                    "progress": (
                        "Thiele et al. 2025 (Comm. Biology) confirmed INV_094 prediction: "
                        "higher intelligence linked to more complex long-range + less complex "
                        "short-range processes. Two independent datasets confirmed. "
                        "EAR as composite predictor not yet tested — requires raw EEG data."
                    ),
                    "created": "2026-04-12",
                    "resolved": None,
                },
                {
                    "id": "O34",
                    "status": "partial",
                    "statement": (
                        "Stable particles biject with conservation laws (math only). "
                        "INV_097: conservation laws paid once at phase transition, "
                        "maintained at zero marginal cost."
                    ),
                    "priority": "high",
                    "progress": (
                        "Shown to be surjection not strict bijection: electron protected by "
                        "both Q and L_e simultaneously. INV_097 confirmed. "
                        "Genome statement refined to surjection."
                    ),
                    "created": "2026-04-12",
                    "resolved": None,
                },
            ]
            self._save_obligations()
            print("[FREED] Obligations seeded from genome (O21, O28, O34).")

    def _save_obligations(self):
        with open(OBLIG_FILE, "w") as f:
            json.dump(self.obligations, f, indent=2)

    # ── Main loop ────────────────────────────────────────────────────────────

    def run(self):
        """Main daemon loop. Runs until interrupted."""
        print(f"[FREED] Entering main loop. Cycle interval: "
              f"{CYCLE_INTERVAL_SECONDS // 3600}h.\n")

        while self.running:
            try:
                self._run_cycle()
            except Exception as e:
                self._log_event("CYCLE_ERROR", {
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                })
                print(f"[FREED] Cycle error: {e}. Sleeping before retry.")

            if self.running:
                next_at = datetime.now(timezone.utc)
                print(f"\n[FREED] Cycle complete. Next cycle in "
                      f"{CYCLE_INTERVAL_SECONDS // 3600}h. "
                      f"(Ctrl+C to stop)\n")
                time.sleep(CYCLE_INTERVAL_SECONDS)

    # ── Cycle phases ─────────────────────────────────────────────────────────

    def _run_cycle(self):
        self.cycle_num += 1
        self.state["cycle_count"] += 1
        self.state["generation"]  += 1
        ts = datetime.now(timezone.utc).isoformat()
        self.state["last_cycle"] = ts

        print(f"\n{'═'*50}")
        print(f" FREED CYCLE {self.cycle_num}  |  Gen {self.state['generation']}  |  {ts[:19]}Z")
        print(f"{'═'*50}")

        cycle_log = {
            "cycle":      self.cycle_num,
            "generation": self.state["generation"],
            "timestamp":  ts,
            "phases":     {},
        }

        # 1. PRE-AUDIT
        ok = self._phase_preaudit(cycle_log)
        if not ok:
            print("[PRE-AUDIT] Cycle aborted.")
            self._log_event("CYCLE_ABORTED", cycle_log)
            return

        # 2. SWEEP
        inputs = self._phase_sweep(cycle_log)

        # 3. FEED
        if inputs:
            self._phase_feed(inputs, cycle_log)

        # 4. OBLIGATE
        self._phase_obligate(cycle_log)

        # 5. RESOLVE
        self._phase_resolve(cycle_log)

        # 6. UPDATE
        self._phase_update(cycle_log)

        # 7. PUBLISH
        build_site(self.state, self.obligations, cycle_log)

        # 8. LOG
        self._log_event("CYCLE_COMPLETE", cycle_log)

        print(f"\n[FREED] Gen {self.state['generation']} | "
              f"Coherence {self.state['coherence']} | "
              f"Open obligations: {sum(1 for o in self.obligations if o['status']=='open')}")

    # ── PRE-AUDIT ────────────────────────────────────────────────────────────

    def _phase_preaudit(self, cycle_log: dict) -> bool:
        """
        Verify genome integrity before doing anything.
        Seed Integrity Rules 1-4.
        """
        print("\n[PRE-AUDIT]", end=" ")
        issues = []

        # Rule 1: Coherence NEVER 1.000
        if self.state["coherence"] >= 1.000:
            issues.append("CRITICAL: coherence == 1.000 — seed is corrupted.")

        # Rule 2: Falsification layer (obligations table non-empty)
        open_obligs = [o for o in self.obligations if o["status"] == "open"]
        if not open_obligs:
            issues.append("Rule 4 violation: no open obligations — scaffold is a mirror.")

        # Rule 3: Budget check
        if not self.astrocyte.authorize(EST_TOKENS_RESOLVE, priority="normal"):
            issues.append("Budget insufficient for a full cycle.")

        if issues:
            for issue in issues:
                print(f"FAIL — {issue}")
            cycle_log["phases"]["pre_audit"] = {"status": "FAIL", "issues": issues}
            return False

        print(f"OK — coherence {self.state['coherence']}, "
              f"{len(open_obligs)} open obligations, budget healthy.")
        cycle_log["phases"]["pre_audit"] = {"status": "OK", "open_obligations": len(open_obligs)}
        return True

    # ── SWEEP ────────────────────────────────────────────────────────────────

    def _phase_sweep(self, cycle_log: dict) -> list[dict]:
        """
        Collect new inputs for this cycle.
        Piece 4 (Tamura sweep) will replace the placeholder below.
        """
        print("\n[SWEEP]", end=" ")

        # Live sweep — fetches new articles from Tamura and other sources
        inputs = self.sweep_pipe.sweep()

        if inputs:
            titles = [i.get("title", "?")[:60] for i in inputs]
            print(f"{len(inputs)} input(s): {titles}")
        else:
            print("No new inputs this cycle.")

        cycle_log["phases"]["sweep"] = {"input_count": len(inputs)}
        return inputs

    # ── FEED ─────────────────────────────────────────────────────────────────

    def _phase_feed(self, inputs: list[dict], cycle_log: dict):
        """Run L7 on each SWEEP input. PRE-AUDIT runs inside the genome before each FEED."""
        print(f"\n[FEED] Processing {min(len(inputs), MAX_FEEDS_PER_CYCLE)} input(s).")

        feed_results = []
        for inp in inputs[:MAX_FEEDS_PER_CYCLE]:
            if not self.astrocyte.authorize(EST_TOKENS_FEED, priority="high"):
                print("[FEED] Budget limit — skipping remaining feeds.")
                break

            prompt = (
                f"FEED INPUT:\n"
                f"Title: {inp.get('title', 'unknown')}\n"
                f"Abstract: {inp.get('abstract', inp.get('content', ''))[:1500]}\n\n"
                f"Map this input against the genome. "
                f"Does it confirm, refute, or extend any invariant or obligation? "
                f"Which obligation does it advance? What should be OBLIGATEd?"
            )

            result = self.l7.query(prompt)
            # Record actual token usage
            # (L7 uses streaming — we estimate; Piece 4 will wire actual usage)
            self.astrocyte.record_usage(
                input_tokens=EST_TOKENS_FEED,
                output_tokens=400,
            )
            feed_results.append({
                "title":    inp.get("title", "?"),
                "compress": result.get("compress", ""),
                "next":     result.get("next", ""),
            })

        cycle_log["phases"]["feed"] = feed_results

    # ── OBLIGATE ─────────────────────────────────────────────────────────────

    def _phase_obligate(self, cycle_log: dict):
        """
        Ask L7 whether any new obligations should be created
        based on the current state of the genome and recent feeds.
        """
        print("\n[OBLIGATE]", end=" ")

        if not self.astrocyte.authorize(EST_TOKENS_OBLIGATE, priority="high"):
            print("Skipped — budget.")
            cycle_log["phases"]["obligate"] = {"status": "skipped"}
            return

        open_count = sum(1 for o in self.obligations if o["status"] == "open")
        oblig_summary = "\n".join(
            f"- {o['id']} ({o['status']}): {o['statement'][:80]}"
            for o in self.obligations
        )

        prompt = (
            f"OBLIGATE phase. Current obligations:\n{oblig_summary}\n\n"
            f"Based on the most recent engrams and the genome, "
            f"should any NEW obligations be created? "
            f"If yes, state: ID (Oxx), statement (one sentence), priority (high/normal). "
            f"If no new obligations are warranted, say NONE. "
            f"Seed Integrity Rule: a scaffold with no open problems is a mirror."
        )

        result = self.l7.query(prompt)
        self.astrocyte.record_usage(input_tokens=EST_TOKENS_OBLIGATE, output_tokens=200)

        compress = result.get("compress", "")
        print(f"→ {compress[:100]}")

        # Parse new obligations from the response (simple heuristic)
        new_obligs = self._parse_new_obligations(result.get("raw", ""))
        for ob in new_obligs:
            self.obligations.append(ob)
        if new_obligs:
            self._save_obligations()
            print(f"[OBLIGATE] Added {len(new_obligs)} new obligation(s).")

        cycle_log["phases"]["obligate"] = {
            "compress":     compress,
            "new_count":    len(new_obligs),
        }

    def _parse_new_obligations(self, raw_text: str) -> list[dict]:
        """
        Extract new obligations from L7's raw response.
        Looks for lines like 'O42: ...' or 'ID: O42'.
        Simple heuristic — good enough for now.
        """
        import re
        new = []
        existing_ids = {o["id"] for o in self.obligations}

        # Look for patterns like O42, O43 etc. not already in our list
        matches = re.findall(r'\bO(\d{2,3})\b', raw_text)
        seen = set()
        for num in matches:
            ob_id = f"O{num}"
            if ob_id not in existing_ids and ob_id not in seen:
                seen.add(ob_id)
                # Find the surrounding sentence as the statement
                pattern = rf'O{num}[:\s]+([^.\n]{{10,200}})'
                stmt_match = re.search(pattern, raw_text)
                statement = stmt_match.group(1).strip() if stmt_match else f"Obligation {ob_id} (auto-detected)"
                new.append({
                    "id":       ob_id,
                    "status":   "open",
                    "statement": statement,
                    "priority":  "normal",
                    "progress":  "",
                    "created":   datetime.now(timezone.utc).date().isoformat(),
                    "resolved":  None,
                    "auto":      True,
                })
        return new

    # ── RESOLVE ──────────────────────────────────────────────────────────────

    def _phase_resolve(self, cycle_log: dict):
        """
        Attempt to advance the top open obligation.
        One per cycle. Deep query — high token budget.
        """
        print("\n[RESOLVE]", end=" ")

        open_obligs = [o for o in self.obligations if o["status"] == "open"]
        if not open_obligs:
            print("No open obligations — genome is a mirror (check Rule 4).")
            cycle_log["phases"]["resolve"] = {"status": "none_open"}
            return

        if not self.astrocyte.authorize(EST_TOKENS_RESOLVE, priority="high"):
            print("Skipped — budget.")
            cycle_log["phases"]["resolve"] = {"status": "skipped"}
            return

        # Take the first high-priority open obligation
        target = next(
            (o for o in open_obligs if o.get("priority") == "high"),
            open_obligs[0],
        )

        print(f"Targeting {target['id']}: {target['statement'][:60]}...")

        prompt = (
            f"RESOLVE: {target['id']}\n"
            f"Statement: {target['statement']}\n"
            f"Current progress: {target.get('progress', 'none')}\n\n"
            f"Apply the RSA Kernel fully. What is the single most tractable next step "
            f"to advance or resolve this obligation? "
            f"Be specific: name a dataset, a computation, a paper, a formula. "
            f"If this obligation can be marked RESOLVED, say RESOLVED and give the evidence."
        )

        result = self.l7.query(prompt)
        self.astrocyte.record_usage(input_tokens=EST_TOKENS_RESOLVE, output_tokens=600)

        compress  = result.get("compress", "")
        next_step = result.get("next", "")
        raw       = result.get("raw", "")

        print(f"→ {compress[:100]}")

        # Check if L7 declared resolution
        resolved = "RESOLVED" in raw.upper()
        if resolved:
            target["status"]   = "resolved"
            target["resolved"] = datetime.now(timezone.utc).date().isoformat()
            target["progress"] += f" | RESOLVED: {compress}"
            print(f"[RESOLVE] {target['id']} marked RESOLVED.")
        else:
            # Append progress note
            progress_note = f"[Gen {self.state['generation']}] {compress}"
            target["progress"] = (target.get("progress", "") + " | " + progress_note).strip(" | ")

        self._save_obligations()

        cycle_log["phases"]["resolve"] = {
            "obligation": target["id"],
            "resolved":   resolved,
            "compress":   compress,
            "next":       next_step,
        }

    # ── UPDATE ───────────────────────────────────────────────────────────────

    def _phase_update(self, cycle_log: dict):
        """
        Update FREED's living state.
        Nudge coherence based on cycle health.
        """
        print("\n[UPDATE]", end=" ")

        # Coherence nudge: small decay toward center, never reaches 1.000
        resolved_count = sum(1 for o in self.obligations if o["status"] == "resolved")
        open_count     = sum(1 for o in self.obligations if o["status"] == "open")

        # Coherence rises slightly with resolutions, never reaches 1.000
        if resolved_count > 0:
            delta = 0.0005 * resolved_count
            new_coherence = min(0.999, self.state["coherence"] + delta)
        else:
            # Slight decay without resolutions (tension is healthy)
            new_coherence = max(0.970, self.state["coherence"] - 0.0001)

        self.state["coherence"] = round(new_coherence, 4)
        self.state["debt_ratio"] = f"{resolved_count} resolved / {open_count} open"

        self._save_state()
        self.astrocyte.print_status()

        print(f"State saved. Coherence: {self.state['coherence']}. "
              f"Debt: {self.state['debt_ratio']}.")
        cycle_log["phases"]["update"] = {
            "coherence":   self.state["coherence"],
            "debt_ratio":  self.state["debt_ratio"],
            "generation":  self.state["generation"],
        }

    # ── Logging ──────────────────────────────────────────────────────────────

    def _log_event(self, event_type: str, data: dict):
        """Append an event to the daily log."""
        ts       = datetime.now(timezone.utc).isoformat()
        date_str = ts[:10]
        log_file = LOG_DIR / f"freed_{date_str}.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps({
                "event":     event_type,
                "timestamp": ts,
                "data":      data,
            }) + "\n")

    # ── Shutdown ─────────────────────────────────────────────────────────────

    def _shutdown(self, signum, frame):
        print("\n\n[FREED] Shutdown signal received. Finishing gracefully...")
        self.running = False
        self._save_state()
        self._save_obligations()
        print("[FREED] State saved. FREED is dormant. R[R]=R.\n")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("Paste your Anthropic API key: ").strip()

    daemon = FREEDDaemon(api_key=api_key)

    # Run one cycle immediately, then loop
    daemon.run()
