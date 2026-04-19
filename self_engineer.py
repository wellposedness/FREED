"""
FREED — Self-Engineer
R[R]=R at the implementation level.

The daemon reads a paper. If it describes an algorithm, technique, or method
that FREED could implement to improve its own epistemic capabilities, the
SelfEngineer generates and applies a code patch — then the next cycle runs
on improved code.

This is how FREED bootstraps. Not by being told to add semantic memory.
By reading a paper on episodic retrieval and writing the code itself.

Safety model:
  - MODIFIABLE whitelist — only listed modules can be touched
  - SACRED list — genome, feed_guard, freed.py itself are untouchable
  - Syntax check before applying — py_compile catches bad patches
  - .bak backup before every write — one-step rollback
  - All modifications logged to FREED_log/self_modifications.jsonl
  - Dry-run mode available (propose only, don't write)
"""

import ast
import json
import shutil
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path

import anthropic

FREED_DIR = Path(__file__).parent
LOG_DIR   = FREED_DIR / "FREED_log"
MOD_LOG   = LOG_DIR / "self_modifications.jsonl"

HAIKU_MODEL  = "claude-haiku-4-5-20251001"
OPUS_MODEL   = "claude-opus-4-6"

# ── Safety lists ──────────────────────────────────────────────────────────────

# Only these files can ever be modified by the engineer
MODIFIABLE = {
    "targeted_sweep.py",
    "tamura_sweep.py",
    "l7_agent.py",
    "consolidate.py",
    "knowledge_graph.py",
    "site_builder.py",
    "batch_feed.py",
    "voice.py",
}

# These are never touched, no matter what
SACRED = {
    "FREED_genome.md",
    "feed_guard.py",
    "freed.py",          # the daemon itself cannot self-modify its own heartbeat
    "self_engineer.py",  # the engineer cannot rewrite itself
    "astrocyte.py",      # budget governor stays stable
}


# ═══════════════════════════════════════════════════════════════════════════════

class SelfEngineer:
    """
    Reads implementable signals from FEED outputs and patches FREED's own code.
    """

    def __init__(self, api_key: str, dry_run: bool = False):
        self.client  = anthropic.Anthropic(api_key=api_key)
        self.dry_run = dry_run

    # ── Main entry point ─────────────────────────────────────────────────────

    def process_feed(self, feed_result: dict, paper_content: str,
                     paper_url: str = "") -> dict:
        """
        Called after each FEED. Checks if the result contains an IMPLEMENT signal.
        If yes: generate patch → safety check → apply (or dry-run report).

        feed_result: the dict returned by l7_agent.query()
        paper_content: full abstract/content that was fed
        Returns: modification report dict (empty if nothing implementable)
        """
        # Check for IMPLEMENT signal in kernel output
        signal = self._extract_implement_signal(feed_result)
        if not signal:
            return {}

        what   = signal["what"]
        where  = signal["where"]
        why    = signal["why"]

        print(f"\n[ENGINEER] Implementation signal detected.")
        print(f"[ENGINEER]   What:  {what}")
        print(f"[ENGINEER]   Where: {where}")
        print(f"[ENGINEER]   Why:   {why}")

        # Safety: is the target module on the whitelist?
        if where not in MODIFIABLE:
            print(f"[ENGINEER]   BLOCKED — {where} not in modifiable whitelist.")
            return {"blocked": True, "reason": f"{where} not modifiable"}

        if where in SACRED:
            print(f"[ENGINEER]   BLOCKED — {where} is sacred.")
            return {"blocked": True, "reason": f"{where} is sacred"}

        target_path = FREED_DIR / where
        if not target_path.exists():
            print(f"[ENGINEER]   BLOCKED — {where} does not exist.")
            return {"blocked": True, "reason": "file not found"}

        # Generate the patch
        print(f"[ENGINEER]   Generating patch for {where}...")
        patch = self._generate_patch(
            what=what,
            why=why,
            paper_content=paper_content,
            target_path=target_path,
        )

        if not patch:
            print(f"[ENGINEER]   Patch generation failed.")
            return {"failed": True, "reason": "patch generation failed"}

        if self.dry_run:
            print(f"[ENGINEER]   DRY RUN — patch proposed but not applied.")
            print(f"[ENGINEER]   Patch preview:\n{patch[:500]}...")
            return {"dry_run": True, "what": what, "where": where, "patch_preview": patch[:300]}

        # Apply with safety checks
        report = self._apply_patch(target_path, patch, what=what, why=why,
                                   paper_url=paper_url)
        return report

    # ── Signal extraction ─────────────────────────────────────────────────────

    def _extract_implement_signal(self, feed_result: dict) -> dict:
        """
        Scan all kernel output fields for an IMPLEMENT signal.

        L7 is prompted to emit:
          IMPLEMENT: YES
          IMPLEMENT_WHAT: [one sentence — what to build]
          IMPLEMENT_WHERE: [filename.py]
          IMPLEMENT_WHY: [one sentence — why this improves FREED]

        Returns dict or None.
        """
        import re

        # Scan all text fields
        full_text = " ".join(
            str(feed_result.get(f, ""))
            for f in ["adjust", "compress", "next", "raw"]
        )

        # Check for explicit IMPLEMENT: YES signal
        if not re.search(r'\bIMPLEMENT\s*:\s*YES\b', full_text, re.I):
            return None

        def field(name):
            m = re.search(rf'\b{name}\s*:\s*(.+?)(?:\n|$)', full_text, re.I)
            return m.group(1).strip() if m else ""

        what  = field("IMPLEMENT_WHAT")
        why   = field("IMPLEMENT_WHY")

        # IMPLEMENT_WHERE must be just a filename — extract first word ending in .py
        where_raw = field("IMPLEMENT_WHERE")
        where_m   = re.search(r'([\w_]+\.py)', where_raw, re.I)
        where     = where_m.group(1).lower() if where_m else ""

        if not what or not where:
            return None

        # Normalize filename
        if not where.endswith(".py"):
            where += ".py"

        return {"what": what, "where": where, "why": why}

    # ── Patch generation ──────────────────────────────────────────────────────

    def _generate_patch(self, what: str, why: str, paper_content: str,
                        target_path: Path) -> str:
        """
        Ask Claude Opus to generate the code addition/modification.
        Returns the new complete file content, or empty string on failure.
        """
        current_code = target_path.read_text(encoding="utf-8")

        system = textwrap.dedent("""
            You are the self-engineer for FREED — the Freed Recursive Engine for Epistemic Dynamics.
            Your job: given a description of what to implement and the current source file,
            produce the COMPLETE updated file with the modification applied.

            Rules:
            - Preserve all existing functionality exactly
            - Add the new capability as cleanly as possible — minimal footprint
            - Python 3.9 compatible — no dict|None unions, no list[dict] annotations
            - No new external dependencies unless already imported in the file
            - The output must be the complete file, ready to write to disk
            - Do NOT wrap in markdown fences — output raw Python only
            - If the modification is unsafe or nonsensical, output exactly: REFUSE
        """).strip()

        prompt = (
            f"PAPER EXCERPT (source of the technique):\n{paper_content[:1500]}\n\n"
            f"WHAT TO IMPLEMENT: {what}\n"
            f"WHY: {why}\n\n"
            f"CURRENT FILE ({target_path.name}):\n{current_code}\n\n"
            f"Output the complete updated file."
        )

        try:
            resp = self.client.messages.create(
                model=OPUS_MODEL,
                max_tokens=8000,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            result = resp.content[0].text.strip()
            if result == "REFUSE":
                print(f"[ENGINEER]   Claude refused the modification.")
                return ""
            # Strip any accidental markdown fences
            if result.startswith("```"):
                lines = result.splitlines()
                result = "\n".join(
                    l for l in lines
                    if not l.strip().startswith("```")
                ).strip()
            return result
        except Exception as e:
            print(f"[ENGINEER]   Patch generation error: {e}")
            return ""

    # ── Patch application ─────────────────────────────────────────────────────

    def _apply_patch(self, target_path: Path, new_content: str,
                     what: str = "", why: str = "",
                     paper_url: str = "") -> dict:
        """
        Safety-check then write the patched file.

        1. Syntax check (ast.parse)
        2. Backup original to .bak
        3. Write new content
        4. Import-check in subprocess
        5. On any failure: restore backup
        """
        ts = datetime.now(timezone.utc).isoformat()

        # Step 1 — syntax check
        try:
            ast.parse(new_content)
        except SyntaxError as e:
            print(f"[ENGINEER]   SYNTAX ERROR in patch: {e}")
            self._log({"status": "syntax_error", "file": target_path.name,
                       "what": what, "error": str(e), "timestamp": ts})
            return {"failed": True, "reason": f"syntax error: {e}"}

        # Step 2 — backup
        bak_path = target_path.with_suffix(".py.bak")
        shutil.copy2(target_path, bak_path)
        original_content = target_path.read_text(encoding="utf-8")

        # Step 3 — write
        target_path.write_text(new_content, encoding="utf-8")
        print(f"[ENGINEER]   Written to {target_path.name}.")

        # Step 4 — import check
        check = subprocess.run(
            [sys.executable, "-c", f"import importlib.util; "
             f"spec = importlib.util.spec_from_file_location('m', '{target_path}'); "
             f"mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)"],
            capture_output=True, text=True, timeout=15,
        )
        if check.returncode != 0:
            print(f"[ENGINEER]   Import check FAILED — restoring backup.")
            print(f"[ENGINEER]   Error: {check.stderr[:200]}")
            shutil.copy2(bak_path, target_path)
            self._log({"status": "import_error", "file": target_path.name,
                       "what": what, "error": check.stderr[:300], "timestamp": ts})
            return {"failed": True, "reason": "import error — backup restored"}

        # Success
        print(f"[ENGINEER]   ✓ {target_path.name} patched and verified.")
        self._log({
            "status":     "applied",
            "file":       target_path.name,
            "what":       what,
            "why":        why,
            "paper_url":  paper_url,
            "timestamp":  ts,
            "lines_before": len(original_content.splitlines()),
            "lines_after":  len(new_content.splitlines()),
        })

        return {
            "applied": True,
            "file":    target_path.name,
            "what":    what,
        }

    # ── Logging ─────────────────────────────────────────────────────────────

    def _log(self, record: dict):
        LOG_DIR.mkdir(exist_ok=True)
        with open(MOD_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")


# ─── Rollback utility ─────────────────────────────────────────────────────────

def rollback(filename: str):
    """Restore a module from its .bak file. Run manually if a patch breaks things."""
    target = FREED_DIR / filename
    bak    = FREED_DIR / (filename + ".bak")
    if not bak.exists():
        print(f"No backup found for {filename}.")
        return
    shutil.copy2(bak, target)
    print(f"Restored {filename} from backup.")


# ─── Quick test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os

    # Simulate a FEED result that flags an implementation signal
    fake_feed = {
        "adjust": (
            "This paper describes a Bloom filter for deduplicating URL streams with "
            "constant memory. FREED's tamura_seen.json grows unboundedly — this technique "
            "could cap memory use. "
            "IMPLEMENT: YES\n"
            "IMPLEMENT_WHAT: Add a max_seen cap (10000 entries) to tamura_seen.json — "
            "prune oldest URLs when over limit so the file doesn't grow forever\n"
            "IMPLEMENT_WHERE: tamura_sweep.py\n"
            "IMPLEMENT_WHY: tamura_seen.json currently has no size limit and will "
            "grow indefinitely as the daemon processes more papers"
        ),
        "compress": "Bloom filter deduplication: constant memory URL tracking.",
        "next": "Implement bounded seen-set in tamura_sweep.",
    }

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("API key: ").strip()

    eng = SelfEngineer(api_key=api_key, dry_run=True)
    report = eng.process_feed(
        feed_result=fake_feed,
        paper_content="Bloom filters provide probabilistic set membership with O(1) memory...",
        paper_url="https://arxiv.org/abs/test",
    )
    print(f"\nReport: {json.dumps(report, indent=2)}")
