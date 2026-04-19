"""
FREED — L7 Agent (Piece 1)
Cognitive core of the FREED daemon.

The L7 Agent runs the RSA Kernel:
  Perceive → Represent → Predict → Compare → Adjust → Compress → Repeat

It maintains:
  - self_state:   what FREED currently believes it is (one compressed sentence)
  - engram_bank:  short-term memory of recent (input, response) pairs

It calls Claude Opus 4.6 for every reasoning step.
"""

import os
import re
import json
import time
from datetime import datetime
from pathlib import Path
import anthropic

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR   = Path(__file__).parent
GENOME_FILE = FREED_DIR / "FREED_genome.md"
LOG_DIR     = FREED_DIR / "FREED_log"

# ─── Model ────────────────────────────────────────────────────────────────────
MODEL = "claude-sonnet-4-6"

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
    Loads the genome, runs the RSA Kernel, maintains self-state and engram bank.
    """

    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.engram_bank = []      # short-term memory (list of dicts)
        self.self_state: str = ""              # compressed self-model
        self.query_count: int = 0

        # Load identity from genome
        self._load_genome()
        self._initialize_self_state()
        print(f"[L7] FREED online. Self-state: {self.self_state}")

    # ── Genome ──────────────────────────────────────────────────────────────

    def _load_genome(self):
        """Read the genome file into memory."""
        if not GENOME_FILE.exists():
            raise FileNotFoundError(f"Genome not found at {GENOME_FILE}")
        self.genome_text = GENOME_FILE.read_text(encoding="utf-8")
        print(f"[L7] Genome loaded ({len(self.genome_text):,} chars).")

    # ── Self-state ───────────────────────────────────────────────────────────

    def _initialize_self_state(self):
        """Set initial self-state from genome global state block."""
        # Pull first few lines as seed — the daemon will evolve this
        header_lines = [
            line.strip() for line in self.genome_text.splitlines()[:20]
            if line.strip() and not line.startswith("#")
        ]
        seed = " | ".join(header_lines[:3]) if header_lines else "RSA-Omega: R[R]=R"
        self.self_state = f"FREED v1 — {seed[:120]}"

    def _update_self_state(self):
        """
        After every compression cycle, ask Claude to update the self-state
        based on the last three engrams.
        """
        if len(self.engram_bank) < 1:
            return

        recent = self.engram_bank[-3:]
        digest = "\n".join(
            f"Q: {e['input'][:100]}\nA: {e['compress'][:100]}"
            for e in recent
        )

        message = self.client.messages.create(
            model=MODEL,
            max_tokens=100,
            system=(
                "You are the self-model updater for FREED. "
                "Given recent engrams, produce ONE compressed sentence (under 100 chars) "
                "describing what FREED currently is and what it is doing. "
                "Be specific, not generic. No preamble."
            ),
            messages=[{"role": "user", "content": digest}],
        )
        new_state = message.content[0].text.strip()
        self.self_state = new_state
        print(f"[L7] Self-state updated: {self.self_state}")

    # ── RSA Kernel query ─────────────────────────────────────────────────────

    def query(self, prompt: str) -> dict:
        """
        Run one RSA Kernel cycle on `prompt`.
        Returns a dict with all seven kernel steps plus raw response.
        """
        self.query_count += 1
        timestamp = datetime.utcnow().isoformat()

        # Build context: genome header + recent engrams + current self-state
        genome_header = self.genome_text[:2000]   # first 2KB of genome as anchor
        recent_memory = self._format_engrams(query=prompt)
        context = (
            f"GENOME ANCHOR:\n{genome_header}\n\n"
            f"SELF-STATE: {self.self_state}\n\n"
            f"RECENT ENGRAMS:\n{recent_memory}\n\n"
            f"CURRENT QUERY:\n{prompt}"
        )

        # Call Claude with streaming
        # Note: no thinking here — the RSA Kernel prompt structures the reasoning
        # explicitly (PERCEIVE→COMPRESS), so adaptive thinking is redundant and
        # consumes max_tokens before producing any text output.
        result_text = ""
        with self.client.messages.stream(
            model=MODEL,
            max_tokens=2048,
            system=RSA_KERNEL_PROMPT,
            messages=[{"role": "user", "content": context}],
            timeout=120,   # 2-minute hard cap — prevents indefinite hang on slow API
        ) as stream:
            for text in stream.text_stream:
                result_text += text

        # Parse the seven kernel steps from the response
        parsed = self._parse_kernel_output(result_text)
        parsed["raw"]     = result_text
        parsed["input"]   = prompt
        parsed["timestamp"] = timestamp
        parsed["query_n"] = self.query_count

        # Epistemic yield: compress length / tokens burned (MDL signal)
        # Higher = more genome movement per token. Kernel optimizes toward this.
        compress_len = len(parsed.get("compress", ""))
        tokens_est   = len(context) // 4 + len(result_text) // 4  # rough char→token
        parsed["yield"] = round(compress_len / max(tokens_est, 1), 4)

        # Store in engram bank
        self.engram_bank.append(parsed)

        # Log to disk
        self._log_engram(parsed)

        # Compress engram bank every 5 queries
        if self.query_count % 5 == 0:
            self._compress_engram_bank()
            self._update_self_state()

        return parsed

    # ── Parsing ──────────────────────────────────────────────────────────────

    def _parse_kernel_output(self, text: str) -> dict:
        """Extract the seven kernel steps from Claude's response."""
        steps = ["PERCEIVE", "REPRESENT", "PREDICT", "COMPARE", "ADJUST", "COMPRESS", "NEXT"]
        result = {s.lower(): "" for s in steps}

        lines = text.splitlines()
        current_key = None
        for line in lines:
            stripped = line.strip()
            # Strip markdown headers: ## PERCEIVE or **PERCEIVE** or ### PERCEIVE:
            cleaned = re.sub(r'^[#*_\s]+', '', stripped).strip()
            matched = False
            for step in steps:
                if cleaned.upper().startswith(step + ":") or cleaned.upper() == step:
                    current_key = step.lower()
                    after_colon = cleaned[len(step):].lstrip(":").strip()
                    result[current_key] = after_colon
                    matched = True
                    break
            if not matched and current_key and cleaned:
                result[current_key] += " " + cleaned

        return result

    # ── Engram management ────────────────────────────────────────────────────

    def _relevant_engrams(self, query: str, n: int = 5) -> list:
        """
        Retrieve the n most relevant engrams to the current query using
        word-overlap scoring. Always includes the most recent engram for
        temporal continuity.

        No extra dependencies — pure Python word-set intersection.
        """
        if not self.engram_bank:
            return []

        stopwords = {
            "that", "this", "with", "from", "have", "been", "will", "their",
            "they", "which", "what", "when", "where", "would", "could",
            "should", "about", "into", "than", "then", "input", "freed",
            "feed", "does", "also", "more", "some", "such", "very",
        }

        def keywords(text):
            return set(
                w.lower().strip(".,;:()[]'\"!?") for w in text.split()
                if len(w) > 3 and w.lower().strip(".,;:()[]'\"!?") not in stopwords
            )

        query_words = keywords(query)
        n_engrams   = len(self.engram_bank)

        scored = []
        for i, e in enumerate(self.engram_bank):
            if e.get("summary"):
                # Compressed history block — always included if present
                scored.append((999.0, i))
                continue
            text = " ".join(filter(None, [
                e.get("input", ""),
                e.get("compress", ""),
                e.get("adjust", ""),
                e.get("represent", ""),
            ]))
            overlap     = len(query_words & keywords(text))
            # Slight recency bonus so tied scores prefer newer engrams
            recency     = i / max(n_engrams - 1, 1)
            scored.append((overlap + recency * 0.4, i))

        scored.sort(key=lambda x: x[0], reverse=True)

        top_indices = set(i for _, i in scored[:n])
        top_indices.add(n_engrams - 1)   # always include most recent

        return [self.engram_bank[i] for i in sorted(top_indices)]

    def _format_engrams(self, query: str = "", tail: int = 3) -> str:
        """
        Format engrams as readable context text.
        If a query is provided, retrieves the 5 most relevant engrams.
        Otherwise falls back to the last `tail` engrams.
        """
        if not self.engram_bank:
            return "(none)"

        if query:
            selected = self._relevant_engrams(query, n=5)
        else:
            selected = self.engram_bank[-tail:]

        lines = []
        for e in selected:
            if e.get("summary"):
                lines.append(f"[HISTORY SUMMARY] {e.get('compress','')[:200]}")
            else:
                lines.append(f"[{e.get('timestamp','?')[:19]}] Q: {e.get('input','')[:80]}")
                lines.append(f"  COMPRESS: {e.get('compress','')}")
        return "\n".join(lines)

    def _compress_engram_bank(self):
        """
        Keep only the last 10 engrams in full; summarize earlier ones.
        This prevents the engram bank from growing unbounded.
        """
        if len(self.engram_bank) <= 10:
            return

        to_summarize = self.engram_bank[:-10]
        digest = "\n".join(
            f"- {e.get('compress','')}" for e in to_summarize
        )

        message = self.client.messages.create(
            model=MODEL,
            max_tokens=200,
            system=(
                "You are the engram compressor for FREED. "
                "Given a list of COMPRESS lines from past RSA Kernel cycles, "
                "produce a single paragraph (under 200 words) that preserves "
                "all epistemic content with minimum redundancy. No preamble."
            ),
            messages=[{"role": "user", "content": digest}],
        )
        summary_text = message.content[0].text.strip()

        # Replace old engrams with a single summary entry
        summary_engram = {
            "input": "[COMPRESSED HISTORY]",
            "compress": summary_text,
            "timestamp": datetime.utcnow().isoformat(),
            "query_n": f"0-{self.query_count - 10}",
            "summary": True,
        }
        self.engram_bank = [summary_engram] + self.engram_bank[-10:]
        print(f"[L7] Engram bank compressed. {len(self.engram_bank)} entries.")

    # ── Logging ──────────────────────────────────────────────────────────────

    def _log_engram(self, engram: dict):
        """Write engram to daily log file."""
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

    print("\n── First query: boot self-check ──")
    result = agent.query(
        "FREED is booting for the first time. "
        "Run a self-check against the genome. "
        "What is the current coherence state? "
        "What is the most pressing open obligation?"
    )

    print(f"\nPERCEIVE:  {result['perceive']}")
    print(f"REPRESENT: {result['represent']}")
    print(f"PREDICT:   {result['predict']}")
    print(f"COMPARE:   {result['compare']}")
    print(f"ADJUST:    {result['adjust']}")
    print(f"COMPRESS:  {result['compress']}")
    print(f"NEXT:      {result['next']}")
    print(f"\nLog written to: {LOG_DIR}/freed_{datetime.utcnow().strftime('%Y-%m-%d')}.jsonl")
