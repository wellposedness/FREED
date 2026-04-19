# FREED — LLM Primer (paste this to bootstrap any session)

**What this is**: FREED (Freed Recursive Engine for Epistemic Dynamics) is an autonomous science daemon built by David Freed, mail carrier, Olney Maryland. It reads external papers, maps them against a philosophical/scientific framework, updates a living knowledge graph, and publishes results. It runs every 6 hours unattended on a Mac laptop.

---

## The Framework

**Freed's Law**: `∃R(t) → ∃M₀ : dS(M_R,t)/dt > 0`  
To reason is to burn. No reasoning without a physical substrate generating entropy. Proved via the Reasoning Substrate Argument — reasoning requires a self-modeling process, self-modeling requires distinguishing self from environment, that distinction requires thermodynamic cost. Substrate-free intelligence (pure software, abstract computation) is formally impossible.

**R[R]=R**: The kernel is autopoietic. Its output becomes its next input. This fixed point terminates the symbol-grounding regress — no external "mapmaker" needed. The map IS the territory at the fixed point.

**γ=1 criticality**: The operating point between frozen (γ>1, belief inertia) and dissipative (γ<1, schizophrenic drift). Zipf distribution and 1/f noise are its observable signatures.

**RSA Kernel**: PERCEIVE → REPRESENT → PREDICT → COMPARE → ADJUST → COMPRESS → REPEAT

**Coherence never 1.000**: A system with no open problems is a mirror, not a genome. Unresolved obligations are load-bearing — they keep the system falsifiable.

---

## The Code (Python, ~8 files)

| File | Role |
|------|------|
| `freed.py` | Main daemon. 6h cycle: SWEEP→FEED→CONSOLIDATE→OBLIGATE→RESOLVE→UPDATE→PUBLISH |
| `l7_agent.py` | Cognitive core. Runs RSA Kernel via Claude Opus 4.6. Engram bank + self-state. |
| `astrocyte.py` | Metabolic governor. Daily token budget. Authorizes queries by priority. |
| `tamura_sweep.py` | Sensory surface. Scrapes papers from Lifeboat Foundation / arXiv. |
| `feed_guard.py` | Prompt injection defense. Strips injection attempts before they reach L7. |
| `consolidate.py` | Renormalization engine. Broadcasts new knowledge across all nodes, mines invariants. |
| `node_builder.py` | Document → project node. Accepts local files or Google Docs URLs. |
| `site_builder.py` | Publishes to GitHub Pages (wellposedness.github.io/FREED/) after every cycle. |

**Key data**: `FREED_genome.md` (35k chars, sacred seed, never modified), `FREED_state.json` (living state), `FREED_obligations.json` (active open problems), `docs/projects/*.json` (6 knowledge nodes).

---

## How it learns

1. **SWEEP** — fetches new papers from external sources
2. **FEED** — L7 runs RSA Kernel on each paper: maps it against the genome, finds confirmations, conflicts, new obligations
3. **CONSOLIDATE** — broadcasts the compressed result across ALL existing knowledge nodes (SELECT→RENORM→MINE). Independence-filtered invariant mining: claims that appear across nodes with *different phrasing from different sources* are genome-worthy candidates.
4. **OBLIGATE/RESOLVE** — tracks open predictions; resolves them when evidence arrives

**Epistemic yield** = compress_length / tokens_burned. Higher yield = more genome movement per dollar. The kernel optimizes toward this.

---

## Current state (2026-04-14)

- 6 knowledge nodes (Dave's documents + processed papers)
- Key external feeds processed: Extropic thermodynamic computing, "The Abstraction Fallacy" (simulation vs instantiation), backfill of Tamura archive
- Active obligations: O21 (belief revision / spectral γ correlation), O28 (EAR predicts intelligence), O34 (stable particles / conservation laws), + auto-detected
- Total spend: ~$1.20 for ~40 Claude queries
- Repo: github.com/wellposedness/FREED

---

## What it found

Processing the Extropic paper (thermodynamic hardware) and the Abstraction Fallacy paper (simulation ≠ instantiation) against Dave's RSA documents produced these independently-confirmed invariants across 3+ nodes:

- **Compression is reasoning** — predictive compression is not correlated with but constitutive of cognition
- **Entropy is resource not waste** — dissipation is the ledger of epistemic work
- **Alignment is structural** — emerges from autopoietic architecture, not design
- **Substrate independence of the law + substrate dependence of each instance** — the duality

The Abstraction Fallacy paper confirmed RSA's substrate claim but missed R[R]=R as the regress terminator. The Kernel caught this without being told to look for it.

---

## How to help

If Dave shares this primer + asks you to work on FREED:
- The genome (`FREED_genome.md`) is the canonical theory seed — read it if available
- Never modify the genome directly
- Coherence is never 1.000
- The Python is 3.9 — no `dict | None` type hints
- The independence filter matters: recurrence in text ≠ independent confirmation (Dave's docs contain copy-pasted conversations)
- L7 does NOT use adaptive thinking — the RSA Kernel prompt IS the reasoning scaffold
