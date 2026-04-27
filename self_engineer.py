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
    "site_builder.py",
    "batch_feed.py",
    "voice.py",
    "knowledge_graph.py",  # authorized 2026-04-25; graph_integrity audit criterion enforced
}

# These are never touched, no matter what
SACRED = {
    "FREED_genome.md",
    "feed_guard.py",
    "freed.py",            # the daemon itself cannot self-modify its own heartbeat
    "self_engineer.py",    # the engineer cannot rewrite itself
    "astrocyte.py",        # budget governor stays stable
    "docs/noethers_table.html", # hand-edited Noether's Table page — never overwrite
}

# Public symbols that must survive any patch to a given file.
# A module that imports cleanly but drops these breaks the daemon silently.
REQUIRED_SYMBOLS = {
    "batch_feed.py":      ["fetch_url"],
    "knowledge_graph.py": ["get_graph", "KnowledgeGraph"],
    "l7_agent.py":        ["L7Agent"],
    "consolidate.py":     ["Consolidator"],
    "tamura_sweep.py":    ["TamuraSweep"],
    "targeted_sweep.py":  ["TargetedSweep"],
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
        Ask Claude Opus to generate a surgical str_replace patch.

        The model sees the full file for orientation but outputs only a
        <<<SEARCH>>>/<<<REPLACE>>>/<<<END>>> block — the exact lines to
        change and their replacements. We apply it programmatically and
        return the resulting complete file, which flows into the existing
        safety checks (syntax, truncation, import, symbol, audit).

        Previously this method asked for the complete file rewrite, which
        caused 71% syntax errors: files >400 lines exhausted the token
        budget mid-generation, producing truncated or malformed output.
        Surgical patches cap output at ~50-100 lines regardless of file size.
        """
        current_code = target_path.read_text(encoding="utf-8")
        lines = current_code.splitlines()

        # Build a structure map: module header + all def/class signatures with
        # line numbers. The model uses this to locate the right insertion point
        # without reading every line of implementation.
        header = "\n".join(f"{i+1:4d}  {l}" for i, l in enumerate(lines[:40]))
        sigs = []
        for i, line in enumerate(lines):
            stripped = line.lstrip()
            if stripped.startswith(("def ", "class ", "async def ")):
                start = max(0, i - 1)
                for j in range(start, min(len(lines), i + 2)):
                    sigs.append(f"{j+1:4d}  {lines[j]}")
                sigs.append("      ...")
        structure_map = (
            f"=== MODULE HEADER (lines 1–{min(40, len(lines))}) ===\n{header}\n\n"
            f"=== FUNCTION/CLASS SIGNATURES ===\n" + "\n".join(sigs)
        )

        system = textwrap.dedent("""
            You are the self-engineer for FREED (Freed Recursive Engine for Epistemic Dynamics).
            Generate a SURGICAL str_replace patch — NOT a whole-file rewrite.

            Output format (use exactly these delimiters, nothing else):
            <<<SEARCH>>>
            [verbatim lines from the current file — include 3–5 lines of context
             before/after the change point, enough to be unique in the file]
            <<<REPLACE>>>
            [replacement lines — same indentation as the original]
            <<<END>>>

            Rules:
            - The SEARCH block must appear VERBATIM in the file (whitespace matters)
            - Include enough surrounding context that the block appears exactly once
            - Python 3.9: no dict|None type unions, no list[dict] annotations
            - No new imports unless that import already exists at the top of the file
            - Preserve all existing functionality — touch only what is needed
            - For a new method on a class: SEARCH for the last few lines of the
              preceding method + first line of the next, splice in the new method
            - If the change is unsafe or nonsensical, output exactly: REFUSE
        """).strip()

        prompt = (
            f"FILE: {target_path.name} ({len(lines)} lines)\n"
            f"WHAT TO IMPLEMENT: {what}\n"
            f"WHY: {why}\n\n"
            f"PAPER EXCERPT (technique source):\n{paper_content[:800]}\n\n"
            f"FILE STRUCTURE (use to locate your patch):\n{structure_map[:2500]}\n\n"
            f"FULL FILE:\n{current_code}\n\n"
            f"Output the <<<SEARCH>>>/<<<REPLACE>>>/<<<END>>> patch only."
        )

        try:
            resp = self.client.messages.create(
                model=OPUS_MODEL,
                max_tokens=4000,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
        except Exception as e:
            print(f"[ENGINEER]   Patch generation error: {e}")
            return ""

        if raw.upper() == "REFUSE":
            print(f"[ENGINEER]   Claude refused the modification.")
            return ""

        # Apply the surgical patch — returns complete file content or ""
        return self._apply_str_replace(current_code, raw, target_path.name)

    def _apply_str_replace(self, current_code: str, patch_text: str,
                           filename: str = "?") -> str:
        """
        Parse and apply a <<<SEARCH>>>/<<<REPLACE>>>/<<<END>>> patch.

        Returns the modified complete file content, or empty string if the
        patch cannot be applied (format error, SEARCH not found, ambiguous).
        Failing open to "" means the existing file is never touched.
        """
        import re

        # Strip accidental markdown fences around the patch
        if "<<<SEARCH>>>" not in patch_text and patch_text.startswith("```"):
            inner = "\n".join(
                l for l in patch_text.splitlines()
                if not l.strip().startswith("```")
            )
            patch_text = inner

        m = re.search(
            r'<<<SEARCH>>>\s*\n(.*?)<<<REPLACE>>>\s*\n(.*?)<<<END>>>',
            patch_text,
            re.DOTALL,
        )
        if not m:
            print(f"[ENGINEER]   Patch format error in {filename} — no SEARCH/REPLACE/END blocks.")
            print(f"[ENGINEER]   Raw output head: {patch_text[:200]!r}")
            return ""

        search_text  = m.group(1).rstrip("\n")
        replace_text = m.group(2).rstrip("\n")

        # Exact match
        if search_text in current_code:
            count = current_code.count(search_text)
            if count > 1:
                print(f"[ENGINEER]   SEARCH block in {filename} is not unique ({count} matches) — add more context.")
                return ""
            return current_code.replace(search_text, replace_text, 1)

        # Fallback: normalize trailing whitespace per line (handles editor-introduced spaces)
        def _rstrip_lines(text):
            # type: (str) -> str
            return "\n".join(line.rstrip() for line in text.splitlines())

        norm_code   = _rstrip_lines(current_code)
        norm_search = _rstrip_lines(search_text)

        if norm_search in norm_code:
            count = norm_code.count(norm_search)
            if count > 1:
                print(f"[ENGINEER]   SEARCH block (normalized) in {filename} not unique ({count} matches).")
                return ""
            print(f"[ENGINEER]   Patch applied with trailing-whitespace normalization.")
            return norm_code.replace(norm_search, _rstrip_lines(replace_text), 1)

        print(f"[ENGINEER]   SEARCH block not found in {filename} — patch cannot apply.")
        print(f"[ENGINEER]   SEARCH was:\n{search_text[:250]!r}")
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

        # Step 1b — truncation guard
        # If the target is large and the patch shrinks it by >20%, the model
        # likely ran out of tokens mid-generation. Reject to prevent data loss.
        original_lines = len(target_path.read_text(encoding="utf-8").splitlines())
        new_lines = len(new_content.splitlines())
        if original_lines > 400 and new_lines < original_lines * 0.80:
            msg = (
                f"TRUNCATION GUARD: {target_path.name} has {original_lines} lines; "
                f"patch produces only {new_lines} ({new_lines * 100 // original_lines}%). "
                f"Whole-file rewrite rejected for files >400 lines — use str_replace patches."
            )
            print(f"[ENGINEER]   {msg}")
            self._log({"status": "truncation_rejected", "file": target_path.name,
                       "original_lines": original_lines, "new_lines": new_lines,
                       "what": what, "timestamp": ts})
            return {"failed": True, "reason": msg}

        # Step 2 — backup
        bak_path = target_path.with_suffix(".py.bak")
        shutil.copy2(target_path, bak_path)
        original_content = target_path.read_text(encoding="utf-8")

        # Extra data backup for knowledge_graph.py — a bad patch could corrupt the graph
        # before the import check catches it; the .json is not recoverable from .bak
        if target_path.name == "knowledge_graph.py":
            graph_path = FREED_DIR / "FREED_graph.json"
            if graph_path.exists():
                shutil.copy2(graph_path, graph_path.with_suffix(".json.bak"))

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

        # Step 4b — required-symbol check
        # A patch can break the daemon without a syntax error if it renames or removes
        # a public function that freed.py imports. The import check above only verifies
        # the module loads; this verifies the contract is intact.
        required = REQUIRED_SYMBOLS.get(target_path.name, [])
        for sym in required:
            sym_check = subprocess.run(
                [sys.executable, "-c",
                 f"import importlib.util; "
                 f"spec = importlib.util.spec_from_file_location('m', '{target_path}'); "
                 f"mod = importlib.util.module_from_spec(spec); "
                 f"spec.loader.exec_module(mod); "
                 f"assert hasattr(mod, '{sym}'), '{sym} missing'"],
                capture_output=True, text=True, timeout=15,
            )
            if sym_check.returncode != 0:
                print(f"[ENGINEER]   Symbol check FAILED — '{sym}' missing from {target_path.name}. Restoring backup.")
                shutil.copy2(bak_path, target_path)
                self._log({"status": "symbol_error", "file": target_path.name,
                           "what": what, "missing_symbol": sym,
                           "error": sym_check.stderr[:300], "timestamp": ts})
                return {"failed": True, "reason": f"symbol check — '{sym}' removed or renamed, backup restored"}

        # Step 5 — epistemic audit
        diff_summary = self._make_diff_summary(original_content, new_content)
        audit_verdict, audit_reason = self._audit_patch(
            target_path.name, diff_summary, why
        )
        print(f"[ENGINEER]   AUDIT: {audit_verdict} — {audit_reason}")

        if audit_verdict == "LOOSENS":
            print(f"[ENGINEER]   AUDIT flagged LOOSENS — reverting.")
            shutil.copy2(bak_path, target_path)
            self._log({
                "status":        "audit_reverted",
                "file":          target_path.name,
                "what":          what,
                "why":           why,
                "paper_url":     paper_url,
                "timestamp":     ts,
                "audit_verdict": audit_verdict,
                "audit_reason":  audit_reason,
            })
            return {
                "failed":          True,
                "reason":          f"AUDIT flagged LOOSENS: {audit_reason}",
                "audit_verdict":   audit_verdict,
                "audit_reason":    audit_reason,
                "needs_obligation": True,
                "ob_statement":    (
                    f"Review self-engineer patch {target_path.name} "
                    f"{ts[:19]} — AUDIT flagged stakes reduction: {audit_reason}"
                ),
            }

        # Success
        print(f"[ENGINEER]   ✓ {target_path.name} patched and verified.")
        self._log({
            "status":        "applied",
            "file":          target_path.name,
            "what":          what,
            "why":           why,
            "paper_url":     paper_url,
            "timestamp":     ts,
            "lines_before":  len(original_content.splitlines()),
            "lines_after":   len(new_content.splitlines()),
            "audit_verdict": audit_verdict,
            "audit_reason":  audit_reason,
        })

        return {
            "applied":       True,
            "file":          target_path.name,
            "what":          what,
            "audit_verdict": audit_verdict,
        }

    # ── Epistemic audit ──────────────────────────────────────────────────────

    def _make_diff_summary(self, original: str, new: str) -> str:
        """Compact diff of changed lines only, capped at 600 chars."""
        import difflib
        diff = list(difflib.unified_diff(
            original.splitlines(),
            new.splitlines(),
            lineterm="",
            n=1,
        ))
        # Keep only changed lines and hunk headers, skip file headers
        changed = [l for l in diff
                   if l.startswith(('+', '-', '@'))
                   and not l.startswith(('+++', '---'))]
        summary = "\n".join(changed)
        if len(summary) > 600:
            summary = summary[:600] + "\n... (truncated)"
        return summary or "(no diff — identical content)"

    def _audit_patch(self, filename: str, diff_summary: str,
                     implement_why: str) -> tuple:
        """
        Ask Haiku whether a patch TIGHTENS, LOOSENS, or is NEUTRAL with respect
        to epistemic standards. Sees only the diff — not the full file.

        Tightens: adds a failure mode the daemon can detect, increases specificity,
                  adds a falsification path, reduces a silent assumption.
        Loosens:  adds caching that hides errors, increases smoothing, reduces
                  rejection sensitivity, adds complexity without a new way to be wrong.
        Neutral:  performance, formatting, logging.

        Returns (verdict, reason) — verdict is 'TIGHTENS', 'LOOSENS', or 'NEUTRAL'.
        Fails open to 'NEUTRAL' so audit errors never block valid patches.
        """
        import re
        graph_criterion = ""
        if filename == "knowledge_graph.py":
            graph_criterion = (
                "\n\nEXTRA CRITERION — knowledge_graph.py patches only:\n"
                "Does this change make high-coherence edges HARDER to generate (raises the bar "
                "— e.g. Onsager weighting, entropy cost, stricter edge scoring)? If so, lean TIGHTENS.\n"
                "Does it make coherence EASIER TO FAKE (smooths scores, caches away low-quality "
                "edges, reduces drift penalty)? If so, it MUST be LOOSENS regardless of intent.\n"
                "Mirroring in code form is the failure mode. Coherence must be earned, not smoothed."
            )
        prompt = (
            f"A self-modifying science daemon just patched its own code.\n"
            f"File: {filename}\n"
            f"Reason: {implement_why}\n\n"
            f"Diff (changed lines only):\n{diff_summary}\n\n"
            f"Answer only: TIGHTENS, LOOSENS, or NEUTRAL\n"
            f"Then one sentence: what specific consequence of being wrong "
            f"changed, and how.\n"
            f"{graph_criterion}\n"
            f"VERDICT: [TIGHTENS/LOOSENS/NEUTRAL]\n"
            f"REASON: [one sentence, ≤20 words]"
        )
        try:
            resp = self.client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=80,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            m_v = re.search(r'VERDICT\s*:\s*(TIGHTENS|LOOSENS|NEUTRAL)', raw, re.I)
            m_r = re.search(r'REASON\s*:\s*(.+)', raw, re.I)
            verdict = m_v.group(1).upper() if m_v else "NEUTRAL"
            reason  = m_r.group(1).strip()[:200] if m_r else raw[:100]
            return verdict, reason
        except Exception as e:
            print(f"[ENGINEER]   Audit call failed: {e} — defaulting NEUTRAL")
            return "NEUTRAL", f"audit error: {e}"

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
