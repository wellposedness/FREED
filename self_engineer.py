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
    # batch_feed.py is intentionally absent. Diagnosed 2026-05-23: the file has no driver
    # (no __main__, no process_feed, no queue loop) and exports exactly one live symbol
    # (fetch_url, used by freed.py:37). IMPLEMENT signals targeting it were pattern-matching
    # the file's appearance and producing orphans. Removed from IMPLEMENT_WHERE at freed.py:1163;
    # keeping it out of MODIFIABLE for belt-and-suspenders symmetry. See O302 for the audit of
    # nine pre-existing orphans inside the file.
    "voice.py",
    # knowledge_graph.py temporarily pulled 2026-05-24 to prevent a fourth iteration
    # of the confirmation-surplus gate landing while three existing versions
    # (in record_feed, score_all_nodes, challenge_surplus_audit) are unresolved.
    # The auto-edge stub in record_feed was contaminating the daily challenge-edge
    # count by writing synthetic 'challenges' edges into self._edges that the gate
    # then counted as evidence it was working — mirror dynamic at the graph level.
    # Re-add after: (1) delete V3 orphan, (2) decide whether V1 or V2 survives,
    # (3) add downstream consumer that reads the flag for a real decision.
    # "knowledge_graph.py",      # authorized 2026-04-25; graph_integrity audit criterion enforced
    "promote.py",              # autonomous genome promotion; criteria and filter prompt may improve
    "simulation_observer.py",  # CA telemetry source; metrics and thresholds may improve
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

        what       = signal["what"]
        where      = signal["where"]
        why        = signal["why"]
        call_site  = signal.get("call_site", "")

        print(f"\n[ENGINEER] Implementation signal detected.")
        print(f"[ENGINEER]   What:      {what}")
        print(f"[ENGINEER]   Where:     {where}")
        print(f"[ENGINEER]   Call site: {call_site}")
        print(f"[ENGINEER]   Why:       {why}")

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
            call_site=call_site,
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
          IMPLEMENT_CALL_SITE: [EXTEND <fn> | <fn> | <Class.method>]
          IMPLEMENT_WHY: [one sentence — why this improves FREED]

        Signals missing CALL_SITE are rejected — they produce orphan patches
        that the wiring gate reverts anyway, so we drop them earlier.

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

        # IMPLEMENT_CALL_SITE — required. "EXTEND <fn>" means modify in place;
        # plain "<fn>" or "<Class.method>" means add new code and wire from that caller.
        call_site_raw = field("IMPLEMENT_CALL_SITE")
        cs_m = re.search(
            r'(?:EXTEND\s+)?([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?)',
            call_site_raw,
        )
        call_site = cs_m.group(0).strip() if cs_m else ""

        if not what or not where:
            return None
        if not call_site:
            print(f"[ENGINEER]   IMPLEMENT signal rejected — no CALL_SITE field. "
                  f"Standalone helpers are auto-reverted; signal dropped at parse.")
            return None

        # Normalize filename
        if not where.endswith(".py"):
            where += ".py"

        return {"what": what, "where": where, "why": why, "call_site": call_site}

    # ── Patch generation ──────────────────────────────────────────────────────

    def _generate_patch(self, what: str, why: str, paper_content: str,
                        target_path: Path, call_site: str = "") -> str:
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

        call_site: L7's stated invocation point. "EXTEND <fn>" → modify that
        function in place. Otherwise the patch must add the new code AND a
        call to it from <fn>/<Class.method> in the same SEARCH/REPLACE block.
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

            ORPHAN-WIRING GATE (deterministic, runs before the audit):
            A patch that adds any new `def` with zero call sites in the project
            is auto-reverted. To pass the gate, your single SEARCH/REPLACE block
            MUST contain both the new code AND its invocation.

            Two acceptable shapes:
              1. EXTEND <fn>: SEARCH the existing function body. REPLACE with the
                 modified body that performs the new behavior in line. No new def.
              2. ADD + WIRE: SEARCH a span that includes (a) the insertion point
                 for the new def AND (b) the existing caller. REPLACE inserts the
                 new def AND adds the call from the existing caller. One block,
                 wider scope. The orphan checker counts references file-wide, so
                 wiring in the same patch satisfies it.

            Do not propose standalone scoring/diagnostic helpers — they have no
            caller and will be reverted on sight.
        """).strip()

        call_site_directive = (
            f"\nCALL SITE (from L7): {call_site}\n"
            f"If prefixed EXTEND, modify that function in place.\n"
            f"Otherwise, your REPLACE block must invoke the new code from that "
            f"function/method.\n"
            if call_site else ""
        )

        prompt = (
            f"FILE: {target_path.name} ({len(lines)} lines)\n"
            f"WHAT TO IMPLEMENT: {what}\n"
            f"WHY: {why}\n"
            f"{call_site_directive}\n"
            f"PAPER EXCERPT (technique source):\n{paper_content[:800]}\n\n"
            f"FILE STRUCTURE (use to locate your patch):\n{structure_map[:2500]}\n\n"
            f"FULL FILE:\n{current_code}\n\n"
            f"Output the <<<SEARCH>>>/<<<REPLACE>>>/<<<END>>> patch only."
        )

        try:
            resp = self.client.messages.create(
                model=OPUS_MODEL,
                max_tokens=8000,
                system=system,
                messages=[{"role": "user", "content": prompt}],
                stop_sequences=["<<<END>>>"],
            )
            raw = resp.content[0].text.strip()
            # The stop sequence is excluded from the response — append it back
            # so the SEARCH/REPLACE/END regex in _apply_str_replace can match.
            # This is the structural fix for knowledge_graph.py truncation:
            # Opus stops cleanly at end-of-patch instead of dribbling into
            # prose that eats max_tokens before <<<END>>> is emitted.
            if resp.stop_reason == "stop_sequence" and "<<<SEARCH>>>" in raw and "<<<REPLACE>>>" in raw:
                raw = raw + "\n<<<END>>>"
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

        # Strip markdown fences unconditionally — model sometimes wraps the block
        cleaned = "\n".join(
            l for l in patch_text.splitlines()
            if not l.strip().startswith("```")
        ) if patch_text.startswith("```") else patch_text

        # Try strict delimiters, then lenient fallbacks for common model mistakes
        m = None
        _matched_variant = "strict"
        for _pattern, _variant in [
            (r'<<<SEARCH>>>\s*\n(.*?)<<<REPLACE>>>\s*\n(.*?)<<<END>>>', "strict"),
            (r'<<SEARCH>>\s*\n(.*?)<<REPLACE>>\s*\n(.*?)<<END>>',       "2-bracket"),
            (r'<SEARCH>\s*\n(.*?)<REPLACE>\s*\n(.*?)<END>',             "1-bracket"),
        ]:
            m = re.search(_pattern, cleaned, re.DOTALL)
            if m:
                _matched_variant = _variant
                break

        if not m:
            print(f"[ENGINEER]   Patch format error in {filename} — no SEARCH/REPLACE/END blocks.")
            print(f"[ENGINEER]   Raw output head: {patch_text[:200]!r}")
            self._log({
                "status": "format_error",
                "file": filename,
                "raw_head": patch_text[:200],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            return ""

        if _matched_variant != "strict":
            print(f"[ENGINEER]   Patch format: lenient match ({_matched_variant}) for {filename}.")

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

        # Step 4c — orphan wiring check (deterministic, pre-audit)
        # If a patch adds a new def with zero call sites anywhere, it's dead code.
        # The Haiku audit sees only the diff, not the full file, so it cannot detect this.
        # Reject orphan patches here before they accrete in knowledge_graph.py et al.
        is_orphan, orphan_names, orphan_reason = self._check_orphan_patch(
            target_path.name, original_content, new_content
        )
        if is_orphan:
            print(f"[ENGINEER]   ORPHAN check FAILED — {orphan_reason}. Reverting.")
            shutil.copy2(bak_path, target_path)
            self._log({
                "status":        "orphan_reverted",
                "file":          target_path.name,
                "what":          what,
                "why":           why,
                "paper_url":     paper_url,
                "timestamp":     ts,
                "orphan_names":  orphan_names,
                "orphan_reason": orphan_reason,
            })
            return {
                "failed":          True,
                "reason":          f"ORPHAN: {orphan_reason}",
                "needs_obligation": True,
                "ob_statement":    (
                    f"Self-engineer attempted to add unwired function(s) to "
                    f"{target_path.name}: {', '.join(orphan_names[:5])}. "
                    f"Either wire each new def into a call site in the same patch, "
                    f"or revise the IMPLEMENT signal to extend an existing call path."
                ),
            }

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

    # ── Orphan wiring check ──────────────────────────────────────────────────

    def _check_orphan_patch(self, filename: str, original: str, new: str):
        """
        Deterministic gate: reject patches that add a def with no call site anywhere.

        Returns (is_orphan: bool, orphan_names: list, reason: str).
        Fails open: any internal error returns False (do not block valid patches).

        Detects functions added at module scope and methods added inside any class.
        For each newly-added name, scans:
          - the new content of the patched file (def line subtracted), and
          - every other *.py in FREED_DIR
        If zero references total, the def is an orphan.
        """
        import re as _re
        try:
            old_tree = ast.parse(original)
            new_tree = ast.parse(new)

            def collect_defs(tree):
                names = set()
                # Module-level functions
                for node in tree.body:
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        names.add(node.name)
                # Class methods (one level deep is sufficient — KG class et al)
                for node in tree.body:
                    if isinstance(node, ast.ClassDef):
                        for sub in node.body:
                            if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                                names.add(sub.name)
                return names

            added = collect_defs(new_tree) - collect_defs(old_tree)
            # Skip dunder methods (they are dispatched implicitly by Python)
            added = {n for n in added if not n.startswith("__")}
            if not added:
                return False, [], ""

            # Build cross-module caller blob (all other .py files in FREED_DIR)
            callers_blob = ""
            for fp in FREED_DIR.glob("*.py"):
                if fp.name == filename:
                    continue
                try:
                    callers_blob += fp.read_text(encoding="utf-8")
                except Exception:
                    pass

            orphans = []
            for name in added:
                # References inside the newly-patched file, minus the def line itself
                in_self = len(_re.findall(r'\b' + _re.escape(name) + r'\b', new))
                if _re.search(r'^\s*(?:async\s+)?def\s+' + _re.escape(name) + r'\b',
                              new, _re.MULTILINE):
                    in_self -= 1
                # References anywhere else in the project
                in_ext = len(_re.findall(r'\b' + _re.escape(name) + r'\b', callers_blob))
                if in_self == 0 and in_ext == 0:
                    orphans.append(name)

            if orphans:
                reason = (
                    f"patch adds {len(orphans)} unwired def(s) with zero call sites: "
                    f"{', '.join(sorted(orphans)[:5])}"
                )
                return True, sorted(orphans), reason
            return False, [], ""
        except Exception as e:
            # Fail open — orphan check must never block valid patches on its own bug
            print(f"[ENGINEER]   Orphan check error (failing open): {e}")
            return False, [], ""

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
            f"TIGHTENS: adds a detectable failure mode, increases specificity, "
            f"adds a falsification path, reduces a silent assumption.\n"
            f"LOOSENS:  adds caching that hides errors, smooths scores, reduces "
            f"rejection sensitivity, or adds complexity without a new way to be wrong.\n"
            f"NEUTRAL:  performance, formatting, logging — no change to epistemic stakes.\n"
            f"{graph_criterion}\n"
            f"Respond with EXACTLY these two lines and nothing else:\n"
            f"VERDICT: TIGHTENS\n"
            f"REASON: one sentence ≤20 words\n"
            f"(Replace TIGHTENS with LOOSENS or NEUTRAL as appropriate. "
            f"Do NOT start the REASON line with a verdict word.)"
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

            # Haiku sometimes skips the VERDICT:/REASON: structure and outputs
            # the verdict as a bare word. Detect a verdict keyword at the start
            # of reason or raw output and override accordingly.
            check = (reason if reason else raw).strip()
            head_m = re.match(r'^(TIGHTENS|LOOSENS|NEUTRAL)\b', check, re.I)
            if head_m:
                detected = head_m.group(1).upper()
                if not m_v:
                    verdict = detected
                    print(f"[ENGINEER]   Audit: no VERDICT: field — parsed '{detected}' from raw output.")
                elif detected != verdict:
                    print(f"[ENGINEER]   Audit verdict corrected: {verdict} → {detected} "
                          f"(reason started with verdict word).")
                    verdict = detected
                # Strip the verdict word from reason for clean logging
                tail = check[head_m.end():].lstrip('.\n\r\t :').strip()
                if tail:
                    reason = tail[:200]

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
