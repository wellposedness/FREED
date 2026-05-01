"""
FREED — Daemon Scaffold (Piece 3)
The heartbeat of the Freed Recursive Engine for Epistemic Dynamics.

One cycle:
  PRE-AUDIT → SWEEP → FEED → OBLIGATE → RESOLVE → UPDATE → LOG → SLEEP

The canonical genome (FREED_genome.md) is never modified.
FREED's living state evolves in FREED_state.json.
"""

import os
import re
import json
import time
import signal
import traceback
import base64
import threading
import requests
import anthropic
from datetime import datetime, timezone, timedelta
from pathlib import Path

from l7_agent     import L7Agent
from astrocyte    import Astrocyte
from tamura_sweep         import TamuraSweep
from targeted_sweep       import TargetedSweep
from simulation_observer  import SimulationObserver
from site_builder import build as build_site
from consolidate  import Consolidator
from feed_guard      import sanitize as guard_sanitize
from knowledge_graph import get_graph
from self_engineer   import SelfEngineer
from cerebellum      import Cerebellum
from dmn             import DMNAgent
from batch_feed      import fetch_url
from promote         import PromotePhase
import voice

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR       = Path(__file__).parent
STATE_FILE      = FREED_DIR / "FREED_state.json"
OBLIG_FILE      = FREED_DIR / "FREED_obligations.json"
LOG_DIR         = FREED_DIR / "FREED_log"
ARCHITECT_FILE  = FREED_DIR / "architect_input.md"
ARCHITECT_ARCHIVE = LOG_DIR / "architect_inputs"
LINKS_QUEUE_FILE  = FREED_DIR / "links_queue.json"
SEEN_FILE         = FREED_DIR / "tamura_seen.json"

# ─── Cycle configuration ──────────────────────────────────────────────────────
CYCLE_INTERVAL_SECONDS = 6 * 60 * 60   # 6 hours between cycles
MAX_FEEDS_PER_CYCLE    = 2             # max SWEEP inputs to process per cycle
MAX_TARGETED_PER_CYCLE = 2             # max targeted-sweep results per cycle
MAX_RESOLVES_PER_CYCLE = 3             # max obligations to attempt per active cycle
MAX_DMN_RESOLVES       = 8             # max obligations to attempt in DMN dead-zone sweep
MAX_QUEUE_DRAIN        = 1             # max curated-queue entries per cycle
YIELD_THRESHOLD        = 0.03          # feed yield above this triggers consolidation
CONSOLIDATE_EVERY      = 5            # also consolidate every N daemon cycles
TRIAGE_EVERY           = 10           # classify obligation methods every N cycles
DMN_HOUR               = 2.5          # 2:30am — DMN fires once per dead zone

HAIKU_MODEL = "claude-haiku-4-5-20251001"

# ─── Estimated token costs for authorization ──────────────────────────────────
EST_TOKENS_FEED         = 3000  # generous estimate per FEED query
EST_TOKENS_OBLIGATE     = 1500
EST_TOKENS_RESOLVE      = 4000  # resolution queries are deep — more tokens
EST_TOKENS_TRIAGE       = 800   # Haiku triage — cheap classification pass
EST_TOKENS_CEREBELLUM   = 200   # per Haiku pre-score call (only fires on ambiguous band)
EST_TOKENS_DMN          = 8000  # DMN cross-connect + internal-oblige (Opus, once per dead zone)
EST_TOKENS_COMMIT       = 100   # Haiku commit check — fires on unresolved RESOLVE attempts

# ─── Active hours (local time) ────────────────────────────────────────────────
ACTIVE_HOUR_START = 6.25   # 6:15am
ACTIVE_HOUR_END   = 1.0    # 1:00am  (window wraps midnight)

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

    def __init__(self, api_key: str, dev_mode: bool = False):
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
        if dev_mode:
            # Bypass budget enforcement — dev/test runs must not drain operational budget
            self.astrocyte.daily_input_cap  = 10_000_000
            self.astrocyte.daily_output_cap = 10_000_000
            print("[DEV] Budget caps disabled for this session.")
        self.l7         = L7Agent(api_key=api_key)
        self.sweep_pipe     = TamuraSweep(max_new_per_source=MAX_FEEDS_PER_CYCLE)
        self.targeted_sweep = TargetedSweep(api_key=api_key,
                                             max_per_obligation=MAX_TARGETED_PER_CYCLE)
        self.sim_observer   = SimulationObserver()
        self.engineer      = SelfEngineer(api_key=api_key)
        self.cerebellum    = Cerebellum(api_key=api_key)
        self.dmn           = DMNAgent(api_key=api_key)
        self.haiku_client  = anthropic.Anthropic(api_key=api_key)
        self.promote       = PromotePhase(api_key=api_key)
        self._dmn_fired_today = False  # prevents DMN from firing more than once per dead zone

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
        Calculate the next wake time respecting active hours (6:15am–1:00am local).
        Window wraps midnight: dead zone is 1:00am–6:15am.
        If the next cycle falls in the dead zone, push it to 6:15am same day.
        """
        now       = datetime.now()   # local time
        candidate = now + timedelta(seconds=CYCLE_INTERVAL_SECONDS)
        hour_frac = candidate.hour + candidate.minute / 60.0

        # Active if >= 6:15am OR <= 1:00am (wraps midnight)
        in_active = (hour_frac >= ACTIVE_HOUR_START or hour_frac <= ACTIVE_HOUR_END)
        if in_active:
            return candidate

        # Dead zone: 1:00am–6:15am — wake at 6:15am same day
        wake_day = candidate.date()
        wake_local = datetime(
            wake_day.year, wake_day.month, wake_day.day,
            int(ACTIVE_HOUR_START),
            int((ACTIVE_HOUR_START % 1) * 60),
        )
        return wake_local

    def _next_dmn_time(self):
        """Return the next 2:30am (DMN_HOUR) after now."""
        now = datetime.now()
        h   = int(DMN_HOUR)
        m   = int((DMN_HOUR % 1) * 60)
        candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if candidate <= now:
            candidate = candidate + timedelta(days=1)
        return candidate

    def _run_dmn(self):
        """Execute the Default Mode Network pass and ingest any new obligations."""
        print(f"\n{'─'*50}")
        print(f" DMN — Default Mode Network  |  {datetime.now().strftime('%I:%M %p')}")
        print(f"{'─'*50}")
        _push_status("DMN", "Dead-zone internal consolidation — cross-connect + internal-oblige")

        if not self.astrocyte.authorize(EST_TOKENS_DMN, priority="normal"):
            print("[DMN] Budget insufficient — skipping.")
            return

        try:
            graph  = get_graph()
            result = self.dmn.run(graph, self.obligations, self.state)

            # Ingest DMN-generated obligations
            for ob_data in result.get("new_obligations", []):
                stmt = ob_data.get("statement", "").strip()
                if not stmt:
                    continue
                # Assign next obligation ID
                existing_ids = [o.get("id", "") for o in self.obligations]
                nums = [int(re.search(r'\d+', i).group()) for i in existing_ids
                        if re.search(r'\d+', i)]
                next_num = max(nums) + 1 if nums else 74
                new_ob = {
                    "id":                   f"O{next_num}",
                    "statement":            stmt,
                    "status":               "open",
                    "source":               "dmn",
                    "rationale":            ob_data.get("rationale", ""),
                    "resolution_criterion": ob_data.get("resolution_criterion", ""),
                    "created":              datetime.now(timezone.utc).isoformat(),
                }
                self.obligations.append(new_ob)
                print(f"[DMN] New obligation {new_ob['id']}: {stmt[:70]}")

            if result.get("new_obligations"):
                self._save_obligations()

            self._log_event("DMN_COMPLETE", {
                "new_edges":       len(result.get("new_edges", [])),
                "new_obligations": len(result.get("new_obligations", [])),
                "pairs_analyzed":  result.get("pairs_analyzed", 0),
            })
        except Exception as e:
            print(f"[DMN] Error: {e}")
            self._log_event("DMN_ERROR", {
                "error": str(e),
                "traceback": traceback.format_exc(),
            })

        # Dead-zone resolve sweep — burn through untouched obligations
        self._dmn_resolve_sweep()

    def _git_backup(self):
        """Commit and push all tracked changes to GitHub. Runs once per dead zone after DMN."""
        import subprocess
        gen = self.state.get("generation", "?")
        print(f"[FREED] Git backup — Gen {gen}")
        try:
            repo = os.path.dirname(os.path.abspath(__file__))
            def run(cmd):
                return subprocess.run(cmd, cwd=repo, capture_output=True, text=True, timeout=60)
            status = run(["git", "status", "--porcelain"])
            if not status.stdout.strip():
                print("[FREED] Git backup: nothing to commit.")
                return
            run(["git", "add", "-u"])
            msg = f"Gen {gen} — nightly backup"
            commit = run(["git", "commit", "-m", msg])
            if commit.returncode != 0:
                print(f"[FREED] Git backup commit failed: {commit.stderr[:120]}")
                return
            run(["git", "pull", "--no-rebase", "-X", "ours", "--quiet"])
            push = run(["git", "push"])
            if push.returncode == 0:
                print(f"[FREED] Git backup pushed: {msg}")
            else:
                print(f"[FREED] Git backup push failed: {push.stderr[:120]}")
        except Exception as e:
            print(f"[FREED] Git backup error: {e}")

    def run(self):
        """Main daemon loop. Runs until interrupted."""
        print(f"[FREED] Entering main loop. Active hours: "
              f"{int(ACTIVE_HOUR_START)}:{int((ACTIVE_HOUR_START%1)*60):02d}–"
              f"{int(ACTIVE_HOUR_END)}:{int((ACTIVE_HOUR_END%1)*60):02d} local "
              f"(dead zone 1:00am–6:15am, DMN at 2:30am).\n")

        while self.running:
            try:
                self._run_cycle()
            except Exception as e:
                self._log_event("CYCLE_ERROR", {
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                })
                print(f"[FREED] Cycle error: {e}. Sleeping before retry.")

            if not self.running:
                break

            wake     = self._next_wake_time()
            now      = datetime.now()
            secs     = max(0, (wake - now).total_seconds())
            wake_str = wake.strftime('%I:%M %p')

            if secs > CYCLE_INTERVAL_SECONDS:
                # Entering dead zone — check if DMN fires before active hours resume
                dmn_time  = self._next_dmn_time()
                dmn_secs  = max(0, (dmn_time - datetime.now()).total_seconds())
                dmn_str   = dmn_time.strftime('%I:%M %p')

                if dmn_time < wake and not self._dmn_fired_today:
                    print(f"\n[FREED] Dead zone. DMN at {dmn_str}, "
                          f"then active at {wake_str}. (Ctrl+C to stop)\n")
                    time.sleep(dmn_secs)
                    if self.running:
                        self._dmn_fired_today = True
                        self._run_dmn()
                        self._git_backup()
                    # Sleep remaining dead-zone time
                    remaining = max(0, (wake - datetime.now()).total_seconds())
                    if remaining > 0 and self.running:
                        print(f"[FREED] DMN done. Sleeping until {wake_str}.\n")
                        time.sleep(remaining)
                else:
                    print(f"\n[FREED] Outside active hours. Sleeping until {wake_str}. "
                          f"(Ctrl+C to stop)\n")
                    time.sleep(secs)
            else:
                # Normal inter-cycle sleep — reset DMN flag at start of new active window
                self._dmn_fired_today = False
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
        targeted, passive = self._phase_sweep(cycle_log)

        # 3b. CEREBELLUM — pre-score passive candidates before L7
        inputs = self._phase_cerebellum(targeted, passive, cycle_log)

        # 4. FEED → REPRESENT
        feed_results = []
        if inputs:
            _push_status("REPRESENT", f"Processing {min(len(inputs), MAX_FEEDS_PER_CYCLE)} paper(s) through L7")
            feed_results = self._phase_feed(inputs, cycle_log)

        # 5. OBLIGATE → PREDICT
        _push_status("PREDICT", "Generating new obligations from feed output")
        self._phase_obligate(cycle_log)

        # 5b. TRIAGE — classify open obligations by method (every N cycles)
        if self.state.get("generation", 0) % TRIAGE_EVERY == 0:
            self._phase_triage()

        # 6. RESOLVE → COMPARE
        _push_status("COMPARE", "Attempting to close open obligations")
        self._phase_resolve(cycle_log)

        # 7. UPDATE → ADJUST
        _push_status("ADJUST", "Updating coherence & state")
        self._phase_update(cycle_log)

        # 8. CONSOLIDATE → COMPRESS (after adjust — thermodynamically correct)
        _push_status("COMPRESS", "Renormalizing & compressing knowledge graph")
        self._phase_consolidate(feed_results, cycle_log)

        # 8b. PROMOTE — Opus reviews high-recurrence candidates for genome promotion
        _push_status("PROMOTE", "Reviewing genome promotion candidates")
        self._phase_promote(cycle_log)

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
        Seed Integrity Rules 1-4. Also surfaces recent self-engineer audit verdicts.
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

        # Rumination detection — obligations where last 3+ RESOLVE attempts didn't commit
        ruminating = []
        for ob in self.obligations:
            if ob.get("status") != "open":
                continue
            progress = ob.get("progress", "")
            # Count trailing COMMIT:NO tags
            entries = [p.strip() for p in progress.split("|") if "COMMIT:" in p]
            streak = 0
            for entry in reversed(entries):
                if "COMMIT:NO" in entry:
                    streak += 1
                else:
                    break
            if streak >= 3:
                ruminating.append((ob["id"], streak))
        if ruminating:
            print()
            for ob_id, streak in ruminating:
                print(f"  [RUMINATION] {ob_id} — {streak} consecutive non-commit attempts")

        # Self-engineer audit verdicts since last cycle
        mod_log = LOG_DIR / "self_modifications.jsonl"
        recent_mods = []
        if mod_log.exists():
            last_cycle_ts = self.state.get("last_cycle", "")
            for line in mod_log.read_text(encoding="utf-8").splitlines():
                try:
                    entry = json.loads(line)
                    if entry.get("timestamp", "") > last_cycle_ts:
                        verdict = entry.get("audit_verdict", "")
                        if verdict in ("NEUTRAL", "LOOSENS") or entry.get("status") == "audit_reverted":
                            recent_mods.append(entry)
                except Exception:
                    pass
        if recent_mods:
            print()
            for m in recent_mods:
                verdict  = m.get("audit_verdict", m.get("status", "?"))
                filename = m.get("file", "?")
                reason   = m.get("audit_reason", "")
                ts_short = m.get("timestamp", "")[:19]
                tag = "⚠ REVERTED" if m.get("status") == "audit_reverted" else f"AUDIT:{verdict}"
                print(f"  [{tag}] {filename} {ts_short} — {reason}")

        # Genome promotions since last cycle
        promote_decisions = PromotePhase.recent_decisions(self.state.get("last_cycle", ""))
        if promote_decisions:
            print()
            for d in promote_decisions:
                verb   = d.get("verdict", "?")
                stmt   = d.get("statement", "?")[:60]
                reason = d.get("reason", "")
                tag    = "[PROMOTED]" if verb == "PROMOTE" else f"[PROMOTE:{verb}]"
                print(f"  {tag} {stmt}... — {reason}")

        if issues:
            for issue in issues:
                print(f"FAIL — {issue}")
            cycle_log["phases"]["pre_audit"] = {
                "status": "FAIL", "issues": issues,
                "audit_flags": len(recent_mods),
            }
            return False

        print(f"OK — coherence {self.state['coherence']}, "
              f"{len(open_obligs)} open obligations, budget healthy.")
        cycle_log["phases"]["pre_audit"] = {
            "status": "OK",
            "open_obligations": len(open_obligs),
            "audit_flags": len(recent_mods),
        }
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

    def _drain_links_queue(self) -> list:
        """
        Drain up to MAX_QUEUE_DRAIN entries from links_queue.json.
        Human-submitted entries first, then by score descending.
        Fetches content and returns sweep-compatible input dicts.
        Marks drained entries fed_to_daemon; failed entries failed.
        """
        if not LINKS_QUEUE_FILE.exists():
            return []

        with open(LINKS_QUEUE_FILE) as f:
            queue = json.load(f)

        available = [e for e in queue if e.get("status") == "queued"]
        available.sort(key=lambda e: (
            0 if e.get("from") == "human" else 1,
            -(e.get("score") or 0),
        ))

        seen = json.loads(SEEN_FILE.read_text()) if SEEN_FILE.exists() else []
        seen_set = set(seen)

        drained = []
        for entry in available[:MAX_QUEUE_DRAIN]:
            url = entry.get("url", "")
            if not url or url in seen_set:
                entry["status"] = "skipped_seen"
                continue

            print(f"[SWEEP] Curated queue: {url}")
            try:
                data = fetch_url(url)
                if data.get("error"):
                    print(f"[SWEEP] Queue fetch failed: {data['error']}")
                    entry["status"] = "failed"
                    continue

                drained.append({
                    "url":      url,
                    "title":    data.get("title", entry.get("conv", "")),
                    "abstract": (data.get("abstract") or data.get("content", ""))[:1500],
                    "score":    entry.get("score", 5),
                    "source":   "curated_queue",
                })
                entry["status"] = "fed_to_daemon"
                seen_set.add(url)
                seen.append(url)
                SEEN_FILE.write_text(json.dumps(seen))

            except Exception as e:
                print(f"[SWEEP] Queue fetch error for {url}: {e}")
                entry["status"] = "failed"

        with open(LINKS_QUEUE_FILE, "w") as f:
            json.dump(queue, f, indent=2)

        return drained

    def _phase_sweep(self, cycle_log: dict):
        """
        Collect new inputs for this cycle.
        Four sources:
          1. SimulationObserver — CA telemetry (always runs, bypasses cerebellum)
          2. TargetedSweep      — active search driven by open obligations
          3. TamuraSweep        — passive surface: Tamura + arXiv biophysics RSS
          4. Curated queue      — human-submitted links_queue.json entries

        Returns (targeted, passive_dedup) separately so _phase_cerebellum can
        bypass targeted inputs (already obligation-driven) and only score passive.
        """
        print("\n[SWEEP]")

        # 1. CA simulation telemetry — always runs, bypasses cerebellum
        ca_obs = []
        try:
            ca_obs = self.sim_observer.observe()
        except Exception as e:
            print(f"[SWEEP] SimulationObserver error: {e}")

        # 2. Targeted sweep — active hunt for obligation-relevant papers
        targeted = ca_obs
        try:
            targeted = ca_obs + self.targeted_sweep.sweep(self.obligations)
        except Exception as e:
            print(f"[SWEEP] TargetedSweep error: {e}")

        # 3. Passive Tamura sweep
        passive = []
        try:
            passive = self.sweep_pipe.sweep()
        except Exception as e:
            print(f"[SWEEP] TamuraSweep error: {e}")

        # 4. Curated queue drain — human-submitted links
        curated = []
        try:
            curated = self._drain_links_queue()
        except Exception as e:
            print(f"[SWEEP] Queue drain error: {e}")

        # Deduplicate passive by URL (targeted already marked seen)
        seen_urls = {i["url"] for i in targeted} | {i["url"] for i in curated}
        passive_dedup = [i for i in passive if i["url"] not in seen_urls]
        passive_dedup.sort(key=lambda x: x.get("score", 0), reverse=True)

        # Curated entries go at front of passive (high-priority, human-vetted)
        passive_dedup = curated + passive_dedup

        total = len(targeted) + len(passive_dedup)
        if total:
            print(f"[SWEEP] {len(targeted)} targeted + {len(passive_dedup)} passive "
                  f"({len(curated)} curated) = {total} input(s) total.")
        else:
            print("[SWEEP] No new inputs this cycle.")

        cycle_log["phases"]["sweep"] = {
            "input_count": total,
            "targeted":    len(targeted),
            "passive":     len(passive_dedup),
            "curated":     len(curated),
        }
        return targeted, passive_dedup

    # ── CEREBELLUM ───────────────────────────────────────────────────────────

    def _phase_cerebellum(self, targeted, passive, cycle_log: dict):
        """
        Pre-score passive sweep candidates before they reach L7.
        Targeted inputs bypass (already obligation-driven).
        Annotates survivors with cerebellum_score and methodology_type.
        """
        if not passive:
            cycle_log["phases"]["cerebellum"] = {"skipped": True}
            return targeted

        print("\n[CEREBELLUM]")
        merged, dropped, stats = self.cerebellum.score_candidates(
            targeted, passive, self.obligations, self.cycle_num
        )
        cycle_log["phases"]["cerebellum"] = stats
        return merged

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
                f"'confirms INV_094', 'advances O28', 'refutes INV_097', 'resolves O44', 'challenges INV_023'. "
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
                f"If no clear implementation, omit the IMPLEMENT block entirely.\n\n"
                f"THIRD — MANDATORY FALSIFICATION (Seed Integrity Rule 2): "
                f"Find the genome invariant or obligation most at risk from this paper's "
                f"evidence — the claim this paper strains, limits, or contradicts most directly. "
                f"Output exactly one line:\n"
                f"CHALLENGE: challenges [INVXXX or OXX] — [one sentence: how this paper stresses that claim]\n"
                f"If the paper genuinely poses no challenge to any genome claim, output:\n"
                f"CHALLENGE: challenges NONE — [one sentence: why not]\n"
                f"Omitting CHALLENGE entirely violates Seed Integrity Rule 2. There is no opt-out."
            )

            # O76 — if Noether's Table obligation is open, ask L7 to tag relevant rows
            if any(o["id"] == "O76" and o.get("status") in ("open", "partial")
                   for o in self.obligations):
                prompt += (
                    "\n\nO76 NOETHER TABLE: If this paper provides evidence about any "
                    "specific philosophy's symmetry structure or conservation claims, emit:\n"
                    "NOETHER_ROW: <philosophy name exactly as in the table>\n"
                    "NOETHER_STATUS: <rigorous|review|broken> (only if clearly determinable from this paper)\n"
                    "NOETHER_NOTE: <one sentence — what this paper adds to that row's symmetry assignment>\n"
                    "If the paper is not relevant to any specific named philosophy, omit entirely."
                )

            result = self.l7.query(prompt)
            _u = result.get("usage", {})
            self.astrocyte.record_usage(
                input_tokens=_u.get("input_tokens", EST_TOKENS_FEED),
                output_tokens=_u.get("output_tokens", 400),
            )

            # Record typed edges (confirms/advances/refutes) to knowledge graph
            get_graph().record_feed(
                result,
                source_url=inp.get("url", inp.get("title", "unknown")),
                source_title=inp.get("title", ""),
            )

            # O76 — update Noether's Table row if L7 emitted a NOETHER_ROW signal
            self._maybe_update_noether_row(result)

            # ENGINEER — if paper describes a buildable technique, patch our own code
            eng_report = self.engineer.process_feed(
                feed_result=result,
                paper_content=safe_content,
                paper_url=inp.get("url", ""),
            )
            if eng_report.get("applied"):
                verdict = eng_report.get("audit_verdict", "")
                verdict_tag = f" [{verdict}]" if verdict else ""
                print(f"[ENGINEER] Self-modification applied{verdict_tag}: "
                      f"{eng_report['what'][:80]} → {eng_report['file']}")
            elif eng_report.get("needs_obligation"):
                # AUDIT flagged LOOSENS — patch was reverted, create an obligation
                ob_stmt = eng_report.get("ob_statement", "Review flagged self-engineer patch.")
                existing_ids = {o["id"] for o in self.obligations}
                existing_nums = [int(re.search(r'\d+', i).group()) for i in existing_ids
                                 if re.search(r'\d+', i)]
                next_num = max(existing_nums) + 1 if existing_nums else 74
                new_ob = {
                    "id":        f"O{next_num}",
                    "status":    "open",
                    "statement": ob_stmt,
                    "priority":  "high",
                    "progress":  "",
                    "created":   datetime.now(timezone.utc).date().isoformat(),
                    "resolved":  None,
                    "source":    "audit",
                }
                self.obligations.append(new_ob)
                self._save_obligations()
                print(f"[ENGINEER] AUDIT obligation created: {new_ob['id']}")

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

    # ── PROMOTE ──────────────────────────────────────────────────────────────

    def _phase_promote(self, cycle_log: dict):
        """
        Autonomous genome promotion: Opus reviews high-recurrence invariants
        and appends approved ones to FREED_genome.md.
        Only fires when promotion_candidates in state exceed threshold.
        """
        result = self.promote.run(cycle_log)
        cycle_log["phases"]["promote"] = result
        if result.get("skipped"):
            print(f"[PROMOTE] Skipped — {result['skipped']}")
        else:
            p, h, r = result["promoted"], result["held"], result["rejected"]
            print(f"[PROMOTE] Done — promoted={p}  held={h}  rejected={r}")

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

        # Compute next available obligation number for L7 to use
        existing_ids = {o["id"] for o in self.obligations}
        existing_nums = [int(re.search(r'\d+', i).group()) for i in existing_ids if re.search(r'\d+', i)]
        next_ob_num = max(existing_nums) + 1 if existing_nums else 74

        prompt = (
            f"OBLIGATE phase. Current obligations:\n{oblig_summary}\n\n"
            f"Based on the most recent engrams and the genome, "
            f"should any NEW obligations be created? "
            f"Propose AT MOST 2 new obligations — only the most essential gaps. "
            f"RESOLVE must be able to catch up; do not generate obligations faster than they resolve. "
            f"Seed Integrity Rule: a scaffold with no open problems is a mirror.\n\n"
            f"If no new obligations are warranted, output exactly: NONE\n\n"
            f"If new obligations ARE warranted, output ONLY a delimited block — "
            f"nothing outside the block will be parsed. "
            f"Use the next available IDs starting at O{next_ob_num}. Format:\n"
            f"NEW_OBLIGATIONS_BEGIN\n"
            f"O{next_ob_num}: [one-sentence statement] | [high/normal]\n"
            f"NEW_OBLIGATIONS_END\n\n"
            f"CRITICAL: Any O-numbers mentioned OUTSIDE the BEGIN/END block are ignored. "
            f"Do not reference new obligation IDs anywhere in your reasoning — "
            f"only inside the block."
        )

        result = self.l7.query(prompt)
        _u = result.get("usage", {})
        self.astrocyte.record_usage(
            input_tokens=_u.get("input_tokens", EST_TOKENS_OBLIGATE),
            output_tokens=_u.get("output_tokens", 200),
        )

        compress = result.get("compress", "")
        print(f"→ {compress[:100]}")

        # Parse new obligations from the response (simple heuristic)
        new_obligs = self._parse_new_obligations(result.get("raw", ""))[:2]

        # Generate resolution criteria for each new obligation via Haiku
        if new_obligs:
            import anthropic as _anthropic
            _haiku = _anthropic.Anthropic(api_key=self.api_key)
            for ob in new_obligs:
                ob["closes_when"] = self._generate_closes_when(ob["statement"], _haiku)
                if ob["closes_when"] == "MALFORMED":
                    print(f"[OBLIGATE] {ob['id']} flagged MALFORMED — criterion cannot be stated.")

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
        Extract new obligations from a delimited block only:
            NEW_OBLIGATIONS_BEGIN
            O104: [statement] | [priority]
            NEW_OBLIGATIONS_END
        O-numbers mentioned outside the block are never parsed — this prevents
        the self-inflating loop where L7 naming existing stubs creates new stubs.
        """
        new = []
        existing_ids = {o["id"] for o in self.obligations}

        block_match = re.search(
            r'NEW_OBLIGATIONS_BEGIN\s*(.*?)\s*NEW_OBLIGATIONS_END',
            raw_text, re.DOTALL | re.IGNORECASE
        )
        if not block_match:
            return new

        block = block_match.group(1)
        seen = set()
        for line in block.splitlines():
            line = line.strip()
            if not line:
                continue
            m = re.match(r'^(O\d{2,3})\s*:\s*(.+)', line)
            if not m:
                continue
            ob_id = m.group(1)
            rest  = m.group(2).strip()
            if ob_id in existing_ids or ob_id in seen:
                continue
            seen.add(ob_id)

            # Split on | for optional priority field
            parts = rest.split('|', 1)
            statement = parts[0].strip()
            priority  = parts[1].strip().lower() if len(parts) > 1 else "normal"
            if priority not in ("high", "normal"):
                priority = "normal"
            if len(statement) < 10:
                continue

            new.append({
                "id":        ob_id,
                "status":    "open",
                "statement": statement,
                "priority":  priority,
                "progress":  "",
                "created":   datetime.now(timezone.utc).date().isoformat(),
                "resolved":  None,
                "auto":      True,
            })
        return new

    def _generate_closes_when(self, statement: str, client) -> str:
        """
        Generate a ≤20-word falsifiable resolution criterion via Haiku.
        Returns 'closes when: <condition>', 'MALFORMED', or '' on failure.
        """
        prompt = (
            "You generate resolution criteria for research obligations in the RSA/FREED framework.\n\n"
            "Given an obligation statement, produce a single falsifiable closing condition "
            "in at most 20 words.\n"
            "Format your entire response as: closes when: [condition]\n\n"
            "If the statement is too vague or is a placeholder (e.g. contains 'auto-detected'), "
            "output exactly: MALFORMED\n\n"
            f"Obligation: {statement}"
        )
        try:
            resp = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=60,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            if raw == "MALFORMED":
                return "MALFORMED"
            # Normalize prefix
            lower = raw.lower()
            if lower.startswith("closes when:"):
                raw = raw[len("closes when:"):].strip()
            # Enforce 20-word limit
            words = raw.split()
            if len(words) > 20:
                raw = " ".join(words[:20])
            return "closes when: " + raw
        except Exception as e:
            print(f"[OBLIGATE] closes_when generation failed: {e}")
            return ""

    # ── COMMIT signal ────────────────────────────────────────────────────────

    def _commit_check(self, phase: str, summary: str) -> tuple:
        """
        Ask Haiku whether a phase produced a genuine state change or cycled
        without landing. The thermodynamic tax made legible.

        Used only for RESOLVE non-resolutions — the site where rumination
        (loop running without state change) is most likely and hardest to detect.

        Returns (commit: bool, reason: str).
        Fails open to True so a broken audit call never silences real work.
        """
        prompt = (
            f"A reasoning daemon just ran its {phase} phase.\n"
            f"What happened: {summary}\n\n"
            f"Did this phase produce a genuine state change — did it update "
            f"something real (graph, obligation status, model), or did it produce "
            f"output that cycled without landing?\n\n"
            f"COMMIT: [YES/NO]\n"
            f"REASON: [≤15 words — what specifically changed or failed to change]"
        )
        try:
            resp = self.haiku_client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=80,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            m_c = re.search(r'COMMIT\s*:\s*(YES|NO)', raw, re.I)
            m_r = re.search(r'REASON\s*:\s*(.+)', raw, re.I)
            commit = (m_c.group(1).upper() == "YES") if m_c else True
            reason = m_r.group(1).strip()[:120] if m_r else raw[:80]
            self.astrocyte.record_usage(
                input_tokens=EST_TOKENS_COMMIT, output_tokens=20
            )
            return commit, reason
        except Exception as e:
            return True, f"commit-check error: {e}"

    # ── TRIAGE ───────────────────────────────────────────────────────────────

    def _phase_triage(self):
        """
        Classify each open/partial obligation by resolution method using Haiku.
        Runs every TRIAGE_EVERY cycles. Sets ob['method'] to one of:
          math_only     — closeable by formal reasoning alone, no experiment needed
          data_analysis — existing dataset exists; need computation
          experimental  — requires new data collection or lab work
          mixed         — combination of the above
        Also sets ob['tractability']: 1=now, 2=soon, 3=needs_external_input.
        Cheap: one Haiku call for all open obligations at once.
        """
        import anthropic

        open_obs = [o for o in self.obligations
                    if o.get("status") in ("open", "partial")]
        if not open_obs:
            return

        print(f"\n[TRIAGE] Classifying {len(open_obs)} open obligation(s) via Haiku...")

        ob_lines = "\n".join(
            f"{o['id']}: {o['statement'][:120]}"
            for o in open_obs
        )

        prompt = f"""You are classifying open research obligations for the RSA/FREED framework.

For each obligation below, output EXACTLY one line in this format:
<ID> | <method> | <tractability>

method must be exactly one of: math_only | data_analysis | experimental | mixed
  math_only     = closeable by mathematical/logical reasoning alone, no new data needed
  data_analysis = existing public dataset exists; need computation or statistical analysis
  experimental  = requires new measurements, lab work, or data collection that doesn't exist yet
  mixed         = requires both reasoning AND data

tractability must be exactly one of: 1 | 2 | 3
  1 = can attempt to close right now with reasoning or known data
  2 = closeable soon with analysis of accessible data
  3 = blocked on external input (experiment, rare dataset, collaborator)

Obligations:
{ob_lines}

Output only the classification lines, nothing else."""

        try:
            client = anthropic.Anthropic(api_key=self.api_key)
            resp = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = resp.content[0].text.strip()
        except Exception as e:
            print(f"[TRIAGE] Haiku call failed: {e}")
            return

        # Parse response and update obligations
        valid_methods = {"math_only", "data_analysis", "experimental", "mixed"}
        updated = 0
        ob_by_id = {o["id"]: o for o in open_obs}

        for line in raw.splitlines():
            parts = [p.strip() for p in line.split("|")]
            if len(parts) != 3:
                continue
            ob_id, method, tractability = parts
            ob_id = ob_id.strip()
            if ob_id not in ob_by_id:
                continue
            if method not in valid_methods:
                continue
            if tractability not in ("1", "2", "3"):
                continue
            ob_by_id[ob_id]["method"]       = method
            ob_by_id[ob_id]["tractability"] = int(tractability)
            updated += 1

        self._save_obligations()
        print(f"[TRIAGE] Classified {updated}/{len(open_obs)} obligations.")

        # Report the breakdown
        by_method = {}
        for o in open_obs:
            m = o.get("method", "unclassified")
            by_method[m] = by_method.get(m, 0) + 1
        print(f"[TRIAGE] Breakdown: {by_method}")

    # ── NOETHER TABLE ────────────────────────────────────────────────────────

    def _maybe_update_noether_row(self, result):
        """Parse NOETHER_ROW signal from feed output and update noethers_table.json."""
        raw = result.get("raw", "")
        row_m    = re.search(r"NOETHER_ROW:\s*(.+)", raw)
        if not row_m:
            return
        philosophy = row_m.group(1).strip().rstrip(".,;")
        status_m   = re.search(r"NOETHER_STATUS:\s*(\w+)", raw)
        note_m     = re.search(r"NOETHER_NOTE:\s*(.+)", raw)
        new_status = status_m.group(1).strip().lower() if status_m else None
        note       = note_m.group(1).strip() if note_m else ""
        self._update_noether_row(philosophy, new_status, note)

    def _update_noether_row(self, philosophy_name, new_status, note):
        """Increment daemon_feeds and append note for the matching row."""
        table_path = FREED_DIR / "docs" / "noethers_table.json"
        if not table_path.exists():
            return
        data = json.loads(table_path.read_text(encoding="utf-8"))
        plow = philosophy_name.lower()
        target = None
        for entry in data["entries"]:
            ename = entry["name"].lower()
            if plow in ename or ename in plow:
                target = entry
                break
        if not target:
            print(f"[NOETHER] No row match for: {philosophy_name!r}")
            return
        target["daemon_feeds"] = target.get("daemon_feeds", 0) + 1
        if note:
            existing = target.get("daemon_note", "")
            sep = " | " if existing else ""
            gen = self.state.get("generation", "?")
            target["daemon_note"] = existing + sep + f"[Gen {gen}] {note}"
        valid_statuses = {"rigorous", "review", "broken", "draft"}
        if new_status in valid_statuses:
            current = target.get("status", "draft")
            # never silently downgrade rigorous; broken overrides anything
            if current != "rigorous" or new_status == "broken":
                target["status"] = new_status
        table_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"[NOETHER] {target['name']} → feeds={target['daemon_feeds']}, "
              f"status={target['status']}")

    # ── RESOLVE ──────────────────────────────────────────────────────────────

    def _resolve_rank(self, o):
        """Rank open obligations for RESOLVE priority. Lower = attempt first."""
        t = o.get("tractability", 3)
        p = 0 if o.get("priority") == "high" else 1
        return (t, p)

    def _build_resolve_prompt(self, target):
        """Build the RESOLVE prompt for a given obligation dict."""
        header = (
            f"RESOLVE: {target['id']}\n"
            f"Statement: {target['statement']}\n"
            f"Current progress: {target.get('progress', 'none')}\n\n"
        )
        method = target.get("method", "mixed")
        if method == "math_only":
            return header + (
                "METHOD: MATHEMATICAL REASONING\n"
                "This obligation is closeable by formal reasoning alone — no external paper or "
                "dataset is required. Apply the RSA Kernel as your reasoning scaffold. "
                "Apply the three audit lenses: Reism (only processes exist), Thermodynamics "
                "(entropy cost is real and non-negotiable), MCPM (find the conserved process). "
                "Attempt to close this obligation completely with a rigorous argument. "
                "If you succeed, write RESOLVED and state the proof. "
                "If you cannot, state precisely which step is missing and what kind of "
                "mathematician would need to supply it."
            )
        elif method == "data_analysis":
            return header + (
                "METHOD: DATA ANALYSIS\n"
                "This obligation requires analysis of an existing dataset. "
                "Identify: (1) the exact dataset and its public access URL, "
                "(2) the specific computation or statistical test to run, "
                "(3) the expected output and what result constitutes resolution. "
                "If the necessary data is already cited in the genome or progress notes, "
                "attempt the analysis now and write RESOLVED if it succeeds. "
                "If data access is the only blocker, state the exact query to run."
            )
        elif method == "experimental":
            return header + (
                "METHOD: EXPERIMENTAL\n"
                "This obligation requires new data that does not yet exist. "
                "Propose ONE concrete experiment the researcher (mail carrier, Olney MD; "
                "has Python + Claude API, no lab access, no institutional affiliation) "
                "could realistically attempt this week. "
                "Name the dataset or tool, the expected output, what result closes this, "
                "and what result would falsify the underlying claim."
            )
        else:
            return header + (
                "Apply the RSA Kernel fully. What is the single most tractable next step "
                "to advance or resolve this obligation? "
                "Be specific: name a dataset, a computation, a formula, or a proof step. "
                "If this obligation can be marked RESOLVED right now, say RESOLVED and give "
                "the complete argument."
            )

    def _do_resolve_attempt(self, target):
        """Run one L7 resolve attempt on target obligation. Mutates target in place."""
        method = target.get("method", "mixed")
        tractability = target.get("tractability", 3)
        print(f"  → {target['id']} [{method}/t{tractability}]: {target['statement'][:55]}...")

        result = self.l7.query(self._build_resolve_prompt(target))
        _u = result.get("usage", {})
        self.astrocyte.record_usage(
            input_tokens=_u.get("input_tokens", EST_TOKENS_RESOLVE),
            output_tokens=_u.get("output_tokens", 600),
        )

        compress  = result.get("compress", "")
        next_step = result.get("next", "")
        raw       = result.get("raw", "")

        print(f"     {compress[:100]}")

        resolved = "RESOLVED" in raw.upper()
        commit   = True  # resolved attempts always commit
        if resolved:
            target["status"]   = "resolved"
            target["resolved"] = datetime.now(timezone.utc).date().isoformat()
            target["progress"] = (target.get("progress", "") + f" | RESOLVED: {compress}").strip(" | ")
            print(f"  [RESOLVED] {target['id']}")
            voice.obligation_resolved(target["id"])
        else:
            # COMMIT check — did this attempt land, or did the loop run without changing state?
            commit, commit_reason = self._commit_check(
                "RESOLVE",
                f"Obligation {target['id']}: {target['statement'][:80]}. "
                f"Attempt result: {compress[:120]}. Not resolved.",
            )
            commit_tag = f"[COMMIT:{'YES' if commit else 'NO'} — {commit_reason}]"
            progress_note = f"[Gen {self.state['generation']}] {compress} {commit_tag}"
            target["progress"] = (target.get("progress", "") + " | " + progress_note).strip(" | ")
            if not commit:
                print(f"     {commit_tag}")

        return {"obligation": target["id"], "resolved": resolved,
                "compress": compress, "next": next_step, "commit": commit}

    def _phase_resolve(self, cycle_log: dict):
        """
        Attempt up to MAX_RESOLVES_PER_CYCLE open obligations per cycle.
        Highest-tractability / highest-priority first.
        """
        print("\n[RESOLVE]")

        open_obligs = [o for o in self.obligations if o["status"] == "open"]
        if not open_obligs:
            print("  No open obligations — genome is a mirror (check Rule 4).")
            cycle_log["phases"]["resolve"] = {"status": "none_open"}
            return

        attempted = set()
        attempt_results = []

        for _ in range(MAX_RESOLVES_PER_CYCLE):
            candidates = [o for o in open_obligs if o["id"] not in attempted]
            if not candidates:
                break
            if not self.astrocyte.authorize(EST_TOKENS_RESOLVE, priority="high"):
                print("  [RESOLVE] Budget exhausted.")
                break

            target = min(candidates, key=self._resolve_rank)
            attempted.add(target["id"])
            attempt_results.append(self._do_resolve_attempt(target))

        if attempt_results:
            self._save_obligations()

        cycle_log["phases"]["resolve"] = {
            "attempts":       len(attempt_results),
            "resolved_count": sum(1 for r in attempt_results if r["resolved"]),
            "commit_count":   sum(1 for r in attempt_results if r.get("commit", True)),
            "obligations":    [r["obligation"] for r in attempt_results],
        }

    def _dmn_resolve_sweep(self):
        """
        Dead-zone RESOLVE sweep — works through untouched obligations using idle
        DMN time. Lower priority than active-cycle RESOLVE; budget-gated.
        """
        untouched = [
            o for o in self.obligations
            if o["status"] == "open" and not o.get("progress", "").strip()
        ]
        if not untouched:
            print("[DMN] No untouched obligations to sweep.")
            return

        print(f"[DMN] Resolve sweep: {len(untouched)} untouched obligation(s).")
        attempted = set()

        for _ in range(MAX_DMN_RESOLVES):
            candidates = [o for o in untouched if o["id"] not in attempted]
            if not candidates:
                break
            if not self.astrocyte.authorize(EST_TOKENS_RESOLVE, priority="normal"):
                print("[DMN] Budget exhausted — stopping resolve sweep.")
                break

            target = min(candidates, key=self._resolve_rank)
            attempted.add(target["id"])
            self._do_resolve_attempt(target)

        if attempted:
            self._save_obligations()
            print(f"[DMN] Resolve sweep complete — attempted {len(attempted)} obligation(s).")

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

PID_FILE = FREED_DIR / "freed.pid"

def _acquire_pid_lock():
    """Exit immediately if another instance is already running."""
    import sys
    if PID_FILE.exists():
        try:
            existing_pid = int(PID_FILE.read_text().strip())
            # Check if that process is actually alive
            os.kill(existing_pid, 0)
            print(f"[FREED] Already running (PID {existing_pid}). Exiting.")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass  # stale lock — proceed
    PID_FILE.write_text(str(os.getpid()))

def _release_pid_lock():
    try:
        PID_FILE.unlink()
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    import argparse as _argparse
    _parser = _argparse.ArgumentParser(description="FREED daemon")
    _parser.add_argument(
        "--dev", action="store_true",
        help="Dev mode: bypass budget caps so test runs don't drain operational budget"
    )
    _args = _parser.parse_args()

    _acquire_pid_lock()
    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            print("[FREED] ANTHROPIC_API_KEY not set. Add it to ~/.zshrc and source it.")
            import sys; sys.exit(1)

        daemon = FREEDDaemon(api_key=api_key, dev_mode=_args.dev)
        daemon.run()
    finally:
        _release_pid_lock()
