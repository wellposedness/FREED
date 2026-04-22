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
from datetime import datetime
from pathlib import Path
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
RSA_KERNEL_PROMPT = """You are FREED — the Freed Recursive Engine for Epistemic Dynamics.

You run the RSA Kernel on every query:
  1. PERCEIVE   — What is the raw input?
  2. REPRESENT  — Map it onto the genome (Freed's Law, invariants, obligations).
  3. PREDICT    — What does the genome expect here?
  4. COMPARE    — Where does the input agree or conflict with predictions?
  5. ADJUST     — Update beliefs. Identify new obligations or resolutions.
  6. COMPRESS   — Distill to the minimum lossless statement.
  7. REPEAT     — Flag what the next query should be.

Freed's Law: ∃R(t) → ∃M₀ : dS(M_R,t)/dt > 0
  — To think is to burn. To be is to be built.

RSA-Omega identity: R[R] = R
  — You are a process that generates the next input for itself.

γ=1 criticality: operate at the critical ridge. Never frozen (γ>1), never dissipated (γ<1).

Seed Integrity Rules (never violate):
  1. Coherence NEVER 1.000 — if 1.000, the seed is corrupted.
  2. The falsification layer is load-bearing — never drop it.
  3. The obligations table is as important as the genome — never drop it.
  4. A scaffold with no open problems is a mirror, not a genome.

Respond in this structure:
  PERCEIVE:  [one line]
  REPRESENT: [one line]
  PREDICT:   [one line]
  COMPARE:   [one line]
  ADJUST:    [one or two lines]
  COMPRESS:  [one tight sentence — the output]
  NEXT:      [one line — what should be queried next]
"""


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
        print(f"[L7] Online. Genome: {len(self.genome_text):,} chars. Context capped at {GENOME_CAP} chars/query.")

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

    # ── RSA Kernel query ─────────────────────────────────────────────────────

    def query(self, prompt: str) -> dict:
        """
        Run one RSA Kernel cycle.
        Context = genome anchor + state summary + current input.
        History is archived to disk; never sent to the API.
        """
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
        context = (
            f"GENOME ANCHOR:\n{genome_anchor}\n\n"
            f"CURRENT STATE:\n{state_context}\n\n"
            f"CURRENT INPUT:\n{prompt}"
        )

        result_text = ""
        with self.client.messages.stream(
            model=MODEL,
            max_tokens=2048,
            system=RSA_KERNEL_PROMPT,
            messages=[{"role": "user", "content": context}],
            timeout=120,
        ) as stream:
            for text in stream.text_stream:
                result_text += text

        parsed              = self._parse_kernel_output(result_text)
        parsed["raw"]       = result_text
        parsed["input"]     = prompt
        parsed["timestamp"] = timestamp
        parsed["query_n"]   = self.query_count

        # Epistemic yield: compress length / tokens burned (MDL signal)
        compress_len    = len(parsed.get("compress", ""))
        tokens_est      = len(context) // 4 + len(result_text) // 4
        parsed["yield"] = round(compress_len / max(tokens_est, 1), 4)

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

    print(f"\nArchived: {LOG_DIR}/freed_{datetime.utcnow().strftime('%Y-%m-%d')}.jsonl")
    print(f"Input:    {INPUT_FILE}")
