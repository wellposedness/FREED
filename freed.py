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
import base64
import threading
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

from l7_agent     import L7Agent
from astrocyte    import Astrocyte
from tamura_sweep   import TamuraSweep
from targeted_sweep import TargetedSweep
from site_builder import build as build_site
from consolidate  import Consolidator
from feed_guard      import sanitize as guard_sanitize
from knowledge_graph import get_graph
from self_engineer   import SelfEngineer
import voice

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR       = Path(__file__).parent
STATE_FILE      = FREED_DIR / "FREED_state.json"
OBLIG_FILE      = FREED_DIR / "FREED_obligations.json"
LOG_DIR         = FREED_DIR / "FREED_log"
ARCHITECT_FILE  = FREED_DIR / "architect_input.md"
ARCHITECT_ARCHIVE = LOG_DIR / "architect_inputs"

# ─── Cycle configuration ──────────────────────────────────────────────────────
CYCLE_INTERVAL_SECONDS = 6 * 60 * 60   # 6 hours between cycles
MAX_FEEDS_PER_CYCLE    = 2             # max SWEEP inputs to process per cycle
MAX_TARGETED_PER_CYCLE = 2             # max targeted-sweep results per cycle
MAX_RESOLVES_PER_CYCLE = 1             # max obligations to attempt per cycle
YIELD_THRESHOLD        = 0.03          # feed yield above this triggers consolidation
CONSOLIDATE_EVERY      = 5            # also consolidate every N daemon cycles

# ─── Estimated token costs for authorization ──────────────────────────────────
EST_TOKENS_FEED    = 3000   # generous estimate per FEED query
EST_TOKENS_OBLIGATE = 1500
EST_TOKENS_RESOLVE  = 4000  # resolution queries are deep — more tokens

# ─── Active hours (local time) ────────────────────────────────────────────────
ACTIVE_HOUR_START = 5.5    # 5:30am
ACTIVE_HOUR_END   = 23.5   # 11:30pm

# ─── GitHub status push ───────────────────────────────────────────────────────
_GH_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
_GH_REPO   = "wellposedness/FREED"
_GH_PATH   = "docs/status.json"
_GH_API    = f"https://api.github.com/repos/{_GH_REPO}/contents/{_GH_PATH}"
_GH_HEADS  = {"Authorization": f"token {_GH_TOKEN}", "Accept": "application/vnd.github.v3+json"}
_status_sha = None   # cached SHA of last known status.json on GitHub

def _push_status(phase: str, detail: str = ""):
    """Write docs/status.json locally and push via GitHub API (non-blocking)."""
    payload = {
        "phase":     phase,
        "detail":    detail,
        "ts":        datetime.now(timezone.utc).isoformat(),
    }
    # Write locally too (so _write_index can embed it)
    status_path = FREED_DIR / "docs" / "status.json"
    status_path.write_text(json.dumps(payload, indent=2))

    def _push():
        global _status_sha
        try:
            content = base64.b64encode(json.dumps(payload, indent=2).encode()).decode()
            body = {"message": f"status: {phase}", "content": content}
            if _status_sha:
                body["sha"] = _status_sha
            resp = requests.put(_GH_API, json=body, headers=_GH_HEADS, timeout=10)
            if resp.status_code in (200, 201):
                _status_sha = resp.json()["content"]["sha"]
            elif resp.status_code == 409:
                # SHA conflict — fetch current SHA and retry once
                r2 = requests.get(_GH_API, headers=_GH_HEADS, timeout=10)
                if r2.ok:
                    _status_sha = r2.json().get("sha")
                    body["sha"] = _status_sha
                    r3 = requests.put(_GH_API, json=body, headers=_GH_HEADS, timeout=10)
                    if r3.ok:
                        _status_sha = r3.json()["content"]["sha"]
        except Exception as e:
            print(f"[STATUS] Push failed: {e}")

    threading.Thread(target=_push, daemon=True).start()


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
        self.sweep_pipe    = TamuraSweep(max_new_per_source=MAX_FEEDS_PER_CYCLE)
        self.targeted_sweep = TargetedSweep(api_key=api_key,
                                             max_per_obligation=MAX_TARGETED_PER_CYCLE)
        self.engineer      = SelfEngineer(api_key=api_key)

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

    def _next_wake_time(self) -> datetime:
        """
        Calculate the next wake time respecting active hours (5:30am–11:30pm local).
        If the next cycle would fall in the dead zone, push it to 5:30am.
        """
        now      = datetime.now()   # local time
        candidate = now + timedelta(seconds=CYCLE_INTERVAL_SECONDS)
        hour_frac = candidate.hour + candidate.minute / 60.0

        if ACTIVE_HOUR_START <= hour_frac <= ACTIVE_HOUR_END:
            return candidate  # falls in window — use it

        # Candidate is in the dead zone. Wake at 5:30am.
        wake_day = candidate.date()
        if hour_frac > ACTIVE_HOUR_END:
            # Past 11:30pm — wake tomorrow morning
            wake_day = (candidate + timedelta(days=1)).date()
        wake_local = datetime(
            wake_day.year, wake_day.month, wake_day.day,
            int(ACTIVE_HOUR_START),
            int((ACTIVE_HOUR_START % 1) * 60),
        )
        return wake_local

    def run(self):
        """Main daemon loop. Runs until interrupted."""
        print(f"[FREED] Entering main loop. Active hours: "
              f"{int(ACTIVE_HOUR_START)}:{int((ACTIVE_HOUR_START%1)*60):02d}–"
              f"{int(ACTIVE_HOUR_END)}:{int((ACTIVE_HOUR_END%1)*60):02d} local.\n")

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
                wake = self._next_wake_time()
                now  = datetime.now()
                secs = max(0, (wake - now).total_seconds())
                wake_str = wake.strftime('%I:%M %p')
                if secs > CYCLE_INTERVAL_SECONDS:
                    print(f"\n[FREED] Outside active hours. Sleeping until {wake_str}. "
                          f"(Ctrl+C to stop)\n")
                else:
                    print(f"\n[FREED] Cycle complete. Next cycle at {wake_str}. "
                          f"(Ctrl+C to stop)\n")
                time.sleep(secs)

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
        voice.cycle_start(
            self.state['generation'],
            self.state['coherence'],
            sum(1 for o in self.obligations if o['status'] == 'open'),
        )

        cycle_log = {
            "cycle":      self.cycle_num,
            "generation": self.state["generation"],
            "timestamp":  ts,
            "phases":     {},
        }

        # 1. PRE-AUDIT
        _push_status("PRE-AUDIT", f"Gen {self.state['generation']} — checking coherence & budget")
        ok = self._phase_preaudit(cycle_log)
        if not ok:
            print("[PRE-AUDIT] Cycle aborted.")
            _push_status("IDLE", "Cycle aborted — coherence or budget issue")
            self._log_event("CYCLE_ABORTED", cycle_log)
            return

        # 2. ARCHITECT — process any pending directive from Dave / Cowork
        self._phase_architect(cycle_log)

        # 3. SWEEP → PERCEIVE
        _push_status("PERCEIVE", "Searching arXiv & Semantic Scholar for open obligations")
        inputs = self._phase_sweep(cycle_log)

        # 4. FEED → REPRESENT
        feed_results = []
        if inputs:
            _push_status("REPRESENT", f"Processing {min(len(inputs), MAX_FEEDS_PER_CYCLE)} paper(s) through L7")
            feed_results = self._phase_feed(inputs, cycle_log)

        # 5. OBLIGATE → PREDICT
        _push_status("PREDICT", "Generating new obligations from feed output")
        self._phase_obligate(cycle_log)

        # 6. RESOLVE → COMPARE
        _push_status("COMPARE", "Attempting to close open obligations")
        self._phase_resolve(cycle_log)

        # 7. UPDATE → ADJUST
        _push_status("ADJUST", "Updating coherence & state")
        self._phase_update(cycle_log)

        # 8. CONSOLIDATE → COMPRESS (after adjust — thermodynamically correct)
        _push_status("COMPRESS", "Renormalizing & compressing knowledge graph")
        self._phase_consolidate(feed_results, cycle_log)

        # 9. PUBLISH → REPEAT
        _push_status("REPEAT", f"Writing site — Gen {self.state['generation']}")
        build_site(self.state, self.obligations, cycle_log)

        # 10. LOG
        _push_status("IDLE", f"Cycle {self.cycle_num} complete — Gen {self.state['generation']}, coherence {self.state.get('coherence', '?')}")
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

    # ── ARCHITECT ────────────────────────────────────────────────────────────

    def _phase_architect(self, cycle_log: dict):
        """
        Read architect_input.md. If it contains a pending directive from Dave
        or Claude Cowork, feed it to L7 as a highest-priority semantic input,
        then archive and clear the file.

        This is the Cowork → daemon bridge.
        Semantic updates (invariant refinements, new obligations, philosophical
        directives) flow in here without terminal surgery.
        """
        if not ARCHITECT_FILE.exists():
            cycle_log["phases"]["architect"] = {"status": "no_file"}
            return

        raw = ARCHITECT_FILE.read_text(encoding="utf-8").strip()

        # Detect empty / placeholder
        is_empty = (
            not raw or
            "_empty — no directive pending_" in raw or
            len(raw.splitlines()) <= 6  # just the header boilerplate
        )
        if is_empty:
            cycle_log["phases"]["architect"] = {"status": "empty"}
            return

        print("\n[ARCHITECT] Directive detected — processing...")

        prompt = (
            "ARCHITECT DIRECTIVE:\n"
            "The following is a high-priority semantic input from the framework's "
            "architect (David Freed). It may contain invariant refinements, new "
            "obligations, philosophical directives, or genome firmware updates.\n\n"
            "Process it against the genome with full RSA Kernel attention. "
            "Update beliefs, surface new obligations, refine existing invariants. "
            "This input has authority over the current genome state.\n\n"
            f"{raw}"
        )

        if not self.astrocyte.authorize(4000, priority="high"):
            print("[ARCHITECT] Budget insufficient — deferring directive.")
            cycle_log["phases"]["architect"] = {"status": "deferred", "reason": "budget"}
            return

        result = self.l7.query(prompt)
        compress = result.get("compress", "")
        print(f"[ARCHITECT] Processed. COMPRESS: {compress}")

        # Archive the directive with timestamp
        ARCHITECT_ARCHIVE.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive_path = ARCHITECT_ARCHIVE / f"input_{ts}.md"
        archive_path.write_text(
            f"# Architect Input — {ts}\n\n{raw}\n\n---\n\n"
            f"## FREED Response\n\nCOMPRESS: {compress}\n"
            f"NEXT: {result.get('next','')}\n",
            encoding="utf-8",
        )

        # Reset the file to empty state
        ARCHITECT_FILE.write_text(
            "# Architect Input Channel\n\n"
            "Write your directive below and save. "
            "FREED will process it on the next cycle.\n\n"
            "---\n\n"
            "## Current input (replace everything below this line)\n\n"
            "_empty — no directive pending_\n",
            encoding="utf-8",
        )

        print(f"[ARCHITECT] Archived to {archive_path.name}. File reset.")
        cycle_log["phases"]["architect"] = {
            "status":   "processed",
            "compress": compress,
            "archived": str(archive_path.name),
        }

    # ── SWEEP ────────────────────────────────────────────────────────────────

    def _phase_sweep(self, cycle_log: dict) -> list:
        """
        Collect new inputs for this cycle.
        Two sources:
          1. TargetedSweep — active search driven by open obligations (runs first)
          2. TamuraSweep   — passive surface: Tamura + arXiv biophysics RSS
        """
        print("\n[SWEEP]")

        # 1. Targeted sweep — active hunt for obligation-relevant papers
        targeted = []
        try:
            targeted = self.targeted_sweep.sweep(self.obligations)
        except Exception as e:
            print(f"[SWEEP] TargetedSweep error: {e}")

        # 2. Passive Tamura sweep
        passive = []
        try:
            passive = self.sweep_pipe.sweep()
        except Exception as e:
            print(f"[SWEEP] TamuraSweep error: {e}")

        # Merge: targeted first (purposeful), then passive (ambient)
        # Deduplicate by URL — targeted already marked seen, so just filter passive
        seen_urls = {i["url"] for i in targeted}
        passive_dedup = [i for i in passive if i["url"] not in seen_urls]

        inputs = targeted + passive_dedup

        if inputs:
            print(f"[SWEEP] {len(targeted)} targeted + {len(passive_dedup)} passive = "
                  f"{len(inputs)} input(s) total.")
        else:
            print("[SWEEP] No new inputs this cycle.")

        cycle_log["phases"]["sweep"] = {
            "input_count":   len(inputs),
            "targeted":      len(targeted),
            "passive":       len(passive_dedup),
        }
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

            # Second guard layer — defense in depth before content reaches L7
            raw_content = inp.get('abstract', inp.get('content', ''))[:1500]
            guard = guard_sanitize(raw_content, source_url=inp.get('url', ''))
            if guard.dropped:
                print(f"[FEED] DROPPED (injection attempt): {inp.get('title','?')[:60]}")
                continue
            safe_content = guard.clean

            # Build compact obligation reference for graph edge extraction
            open_obs = [o for o in self.obligations
                        if o.get("status", "open") in ("open", "partial")][:8]
            ob_ref = "\n".join(
                f"  {o['id']}: {o.get('statement','')[:80]}"
                for o in open_obs
            ) or "  (none open)"

            prompt = (
                f"FEED INPUT:\n"
                f"Title: {inp.get('title', 'unknown')}\n"
                f"Abstract: {safe_content}\n\n"
                f"OPEN OBLIGATIONS:\n{ob_ref}\n\n"
                f"Map this input against the genome. "
                f"When you identify a relationship, cite the EXACT ID — e.g., "
                f"'confirms INV_094', 'advances O28', 'refutes INV_097', 'resolves O44'. "
                f"Invariant IDs are in the genome (INV_023 through INV_108). "
                f"Obligation IDs are listed above (O28, O34, O44, etc.).\n\n"
                f"SECOND: Does this paper describe a concrete algorithm, technique, or "
                f"method that FREED could implement in its own codebase to improve its "
                f"epistemic capabilities (better search, memory, scoring, representation, "
                f"deduplication, graph structure, etc)? "
                f"If yes, emit exactly:\n"
                f"IMPLEMENT: YES\n"
                f"IMPLEMENT_WHAT: [one sentence — the specific thing to add or change]\n"
                f"IMPLEMENT_WHERE: [the .py filename from: {', '.join(sorted(['targeted_sweep.py','tamura_sweep.py','l7_agent.py','consolidate.py','knowledge_graph.py','site_builder.py','batch_feed.py','voice.py']))}]\n"
                f"IMPLEMENT_WHY: [one sentence — why this improves FREED's epistemic loop]\n"
                f"If no clear implementation, omit the IMPLEMENT block entirely."
            )

            result = self.l7.query(prompt)
            # Record actual token usage
            # (L7 uses streaming — we estimate; Piece 4 will wire actual usage)
            self.astrocyte.record_usage(
                input_tokens=EST_TOKENS_FEED,
                output_tokens=400,
            )

            # Record typed edges (confirms/advances/refutes) to knowledge graph
            get_graph().record_feed(
                result,
                source_url=inp.get("url", inp.get("title", "unknown")),
                source_title=inp.get("title", ""),
            )

            # ENGINEER — if paper describes a buildable technique, patch our own code
            eng_report = self.engineer.process_feed(
                feed_result=result,
                paper_content=safe_content,
                paper_url=inp.get("url", ""),
            )
            if eng_report.get("applied"):
                print(f"[ENGINEER] Self-modification applied: "
                      f"{eng_report['what'][:80]} → {eng_report['file']}")

            compress_text = result.get("compress", "")
            if compress_text:
                voice.compress(compress_text, title=inp.get("title", "")[:40])
            feed_results.append({
                "title":    inp.get("title", "?"),
                "compress": compress_text,
                "next":     result.get("next", ""),
                "yield":    result.get("yield", 0.0),
            })

        cycle_log["phases"]["feed"] = feed_results
        return feed_results

    # ── CONSOLIDATE ──────────────────────────────────────────────────────────

    def _phase_consolidate(self, feed_results: list, cycle_log: dict):
        """
        Renormalization pass. Triggered when:
          - any feed has yield > YIELD_THRESHOLD, OR
          - cycle_count is a multiple of CONSOLIDATE_EVERY
        Broadcasts new knowledge across all existing nodes, then mines
        cross-node invariants. This is what organisms do.
        """
        high_yield = [r for r in feed_results if r.get("yield", 0) > YIELD_THRESHOLD]
        triggered  = bool(high_yield) or (self.state["cycle_count"] % CONSOLIDATE_EVERY == 0)

        if not triggered:
            cycle_log["phases"]["consolidate"] = {"status": "skipped", "reason": "no trigger"}
            return

        trigger_str = "yield" if high_yield else "scheduled"
        print(f"\n[CONSOLIDATE] Triggered ({trigger_str}).")

        # Build new knowledge string from high-yield compresses (or all if scheduled)
        source = high_yield if high_yield else feed_results
        new_knowledge = " ".join(r.get("compress", "") for r in source).strip()

        if not new_knowledge:
            cycle_log["phases"]["consolidate"] = {"status": "skipped", "reason": "no compress"}
            return

        consolidator = Consolidator(self.api_key)
        report = consolidator.run(
            new_knowledge,
            trigger=trigger_str,
            state=self.state,
            obligations=self.obligations,
        )

        cycle_log["phases"]["consolidate"] = report

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
            for ob in new_obligs:
                voice.new_obligation(ob["id"], ob["statement"])

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
            voice.obligation_resolved(target['id'])
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
