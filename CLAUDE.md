# FREED — Claude Code Context

**FREED** = Freed Recursive Engine for Epistemic Dynamics  
**Author**: David Harry Freed, mail carrier, Olney Maryland  
**Repo**: wellposedness/FREED (GitHub Pages at wellposedness.github.io/FREED/)

Autonomous science daemon. Reads external papers → maps against RSA/Freed's Law → updates knowledge graph → publishes static site. Runs every 6 hours unattended.

---

## The Framework

Full theoretical seed: `FREED_genome.md` (v20 canonical, ~35k chars). Core claims:

- **Freed's Law**: `∃R(t) → ∃M₀ : dS(M_R,t)/dt > 0` — to reason is to burn.
- **R[R]=R**: RSA Kernel is autopoietic — generates its own next input.
- **γ=1 criticality**: Zipf distribution and 1/f noise are its signatures.
- **MCPM**: Only Processes Exist — confirmed at 6 independent scales.
- **Coherence NEVER 1.000** — open obligations are load-bearing.

**RSA Kernel**: PERCEIVE → REPRESENT → PREDICT → COMPARE → ADJUST → COMPRESS → REPEAT

---

## Architecture

```
freed.py            — Main daemon. 6h cycle: PRE-AUDIT → SWEEP → FEED → ENGINEER → CONSOLIDATE → OBLIGATE → RESOLVE → UPDATE → PUBLISH
l7_agent.py         — Cognitive core. Claude Opus 4.6. Engram bank with semantic (relevance-based) retrieval. max_tokens=2048.
astrocyte.py        — Metabolic governor. Daily token budget (100k in / 40k out).
tamura_sweep.py     — Passive sensory surface. Tamura/Lifeboat + arXiv biophysics RSS feeds.
targeted_sweep.py   — Active search. Haiku generates arXiv/S2 queries from open obligations. Runs before Tamura.
feed_guard.py       — Prompt injection defense. Two-layer sanitization before content reaches L7.
consolidate.py      — Renormalization engine. SELECT→RENORM→MINE + graph report. Node priority scoring.
knowledge_graph.py  — Typed edge graph. confirms/advances/refutes/resolves edges + node-to-node edges.
self_engineer.py    — Detects IMPLEMENT signals in FEED output, patches MODIFIABLE modules. SACRED list enforced.
node_builder.py     — Document → project node. Auto-generates obligations from NEXT fields via Haiku.
batch_feed.py       — Manual queue processor. Drains links_queue.json through L7.
extract_links.py    — Parses Claude export JSON or plain URL lists into links_queue.json.
site_builder.py     — Static site generator. Semantic color grammar: red=voice, amber=open, blue=partial, green=resolved.
coherence_audit.py  — Symbol registry auditor. Checks against genome_symbols.json.
backfill.py         — Smart Tamura archive backfill.
```

## Key data files

```
FREED_genome.md             — Sacred seed. NEVER modify.
FREED_state.json            — Living state: generation, coherence, cycle count, promotion_candidates.
FREED_obligations.json      — Active obligations (O28–O73+). Open=amber, Partial=blue, Resolved=green.
FREED_graph.json            — Knowledge graph: feed edges + node_edges list.
genome_symbols.json         — Canonical symbol registry (12 terms, recurrence scores).
links_queue.json            — ~279 entries, ~170 queued. Human-submitted + Claude export links.
tamura_seen.json            — URLs already processed. Shared by all sweep sources.
docs/projects.json          — Index of 16 project nodes (as of Apr 2026).
FREED_log/                  — Daily cycle logs, targeted sweep logs, consolidation logs.
FREED_log/self_modifications.jsonl — Every self-modification: applied/failed/refused.
*.py.bak                    — Auto-created before any self-modification.
```

---

## Running FREED

```bash
cd ~/FREED && source ~/.zshrc
python3 -u freed.py                     # daemon (MUST use -u: unbuffered stdout)
python3 batch_feed.py --n 5            # process 5 links from queue
python3 batch_feed.py --academic --n 5 # academic only (score >= 6)
python3 batch_feed.py --stats          # queue status
python3 targeted_sweep.py              # standalone active search test
python3 consolidate.py                 # manual renormalization pass
python3 node_builder.py /path/to.md    # ingest local document
python3 backfill.py                    # process Tamura archive
```

Rollback a self-modification: `python3 -c "from self_engineer import rollback; rollback('filename.py')"`  
`ANTHROPIC_API_KEY` is in `~/.zshrc`. Keep Mac awake: `sudo pmset -a sleep 0 disksleep 0`

---

## Key architectural decisions (don't undo)

- **No adaptive thinking in L7** — consumes all tokens before producing text.
- **max_tokens=2048 in L7** — 1024 was too small.
- **Independence filter in MINE** — `ORIGIN: INDEPENDENT vs SHARED_SOURCE`. Recurrence in text ≠ independent confirmation.
- **Two-layer injection defense** — feed_guard.py called in both tamura_sweep.py and freed.py feed phase.
- **Consolidate trigger** — yield > 0.03 OR every 5th cycle.
- **Python 3.9** — no `dict | None` type hints, no `list[dict]` annotations.
- **Semantic engram retrieval** — `_relevant_engrams()` uses word-overlap scoring + recency bonus, not recency tail alone.
- **targeted_sweep runs before Tamura** — active (purposeful) before passive (ambient).
- **knowledge_graph singleton** — `get_graph()` loaded once per process, flushed after each write.
- **consolidate.py prompt capping** — compress capped at 500 chars, invariants list capped at 15 in renorm calls (prevents runaway latency on large nodes).
- **Wall-clock timeout** — `_api_call()` in consolidate.py wraps `messages.create()` in daemon thread, 120s wall-clock timeout. Fixes httpx per-byte timeout blind spot.

---

## self_engineer.py — Safety model

```python
# Only these files can be patched by the ENGINEER phase:
MODIFIABLE = {
    "targeted_sweep.py", "tamura_sweep.py", "l7_agent.py",
    "consolidate.py", "knowledge_graph.py", "site_builder.py",
    "batch_feed.py", "voice.py",
}

# These are NEVER touched, no matter what:
SACRED = {
    "FREED_genome.md", "feed_guard.py", "freed.py", "self_engineer.py",
    "astrocyte.py", "docs/game_of_life.html",
}
```

Safety pipeline: MODIFIABLE check → SACRED check → `ast.parse()` syntax check → `.bak` backup → write → subprocess import check → restore on failure.

---

## docs/game_of_life.html — Protection (two layers)

This file is **hand-edited** and must never be overwritten by the daemon.

**Layer 1 — site_builder.py** (`_write_game_of_life()`, line ~84):
```python
if target.exists() and "HAND-EDITED" in target.read_text(encoding="utf-8"):
    return
```
The function has an `ENGINEER: DO NOT MODIFY` docstring so the self-engineer won't remove the guard.

**Layer 2 — self_engineer.py SACRED list**: `"docs/game_of_life.html"` is in SACRED — the engineer cannot patch it even if given a direct signal.

**Layer 3 — file sentinel**: First line of game_of_life.html is `<!-- HAND-EDITED: do not overwrite -->`.

All three layers must be broken simultaneously to lose the file. That won't happen.

---

## docs/game_of_life.html — Current simulation state (as of Apr 2026)

**The Game of Truth** — Voronoi foam Mandelbrot CA with six cognitive types.

**Two axes**: loop depth (Builder/Navigator/Drifter) × grounding (Physics/Symbol).  
Type encoding: `type = depth*2 + grounding` (0–5). Helper functions: `typeDepth(t)`, `typeGround(t)`, `makeType(d,g)`.

**Six types and colors**:
| Type | Code | Color | Behavior |
|------|------|-------|----------|
| Physics Builder | 0 | cyan #06b6d4 | Full RSA loop vs territory |
| Symbol Builder | 1 | violet #a855f7 | Full loop vs lagged map |
| Physics Navigator | 2 | green #16a34a | 3-neighbor prediction vs territory |
| Symbol Navigator | 3 | amber #d97706 | 3-neighbor prediction vs map |
| Physics Drifter | 4 | orange #ea580c | Pure noise |
| Symbol Drifter | 5 | rose #f43f5e | Pure noise + divergence tax |

**Symbol grounding mechanics**:
- `SYMBOL_LAG = 0.65` — map updates slowly: `symbolMap[i] = 0.65*symbolMap[i] + 0.35*nextState[i]`
- `GROUND_TAX = 2.5` — energy penalty per unit `|symbolMap[i] - nextState[i]|`
- Symbol types predict well in stable zones; bleed energy at Mandelbrot boundaries

**Mutation on reproduction**: depth axis flips toward Navigator at rate `TYPE_MUT=5%`; grounding flips at `GROUND_MUT=3%`.

**Variable population**: Dead cells stay dead (energy===0). Spontaneous generation ~1 cell per 50 steps (`Math.random()*N_TYPES` for type). No fixed ceiling.

**Hidden physics**: Mandelbrot set zooming toward c=−0.75+0.1i. 7 escape bands. `ZOOM_PER_STEP=0.995`. Zoom level advances every 140 steps.

---

## Project nodes (16 as of Apr 2026)

| ID prefix | Title |
|-----------|-------|
| freed_s_law | Freed's Law — RSA-040 |
| the_reasoning_substrate | The RSA |
| thermodynamic_tightening | Thermodynamic Tightening |
| game_of_life_battery | Game of Life Battery |
| the_minimal_atoms | The Minimal Atoms |
| technical_brief_co_occurring | Co-Occurring Motif Experiments |
| alignment_as_cognitive_gravity | Alignment as Cognitive Gravity |
| the_relational_thermodynamic | Relational Thermodynamic Map |
| mandelbrot_operators | Mandelbrot Operators |
| the_thermodynamic_impossibility | Thermodynamic Impossibility of Omnimax Agent |
| the_quinean_continuity | RSA Section 11 — Quinean Continuity |
| rsa_omega_unified_kernel | RSA-Omega V4.2.1 — State-Packing Kernel |
| the_epistemic_inversion | RSA Section 10 — RCC as Truth Criterion |
| the_rsa_reference_codex | RSA Reference Codex — 73 Papers |
| rsa_omega_kernel_critical_update | RSA-Omega Origin — Core Axioms |
| recursive_semantic_alignment | RSA Complete Paper Taxonomy — 90 Papers |

Auto-obligations O67–O73 generated from node NEXT fields. O71 (completeness proof for RSA kernel loop) is the sharpest.

---

## Active agenda for next session

### 0. FIX KNOWLEDGE GRAPH WIRING (blocks item 1)
Graph shows no edges despite many FEED cycles. Debug: read `freed.py _phase_feed()` — verify `get_graph().record_feed(feed_result, url, title)` is called. Check if `extract_edges()` regex patterns are too restrictive.  
Quick test: `python3 -c "from knowledge_graph import get_graph, extract_edges; r={'raw':'This confirms INV_094 and advances O44'}; print(extract_edges(r,'test','test'))"`

### 1. SUBSTRATE-TYPED EVIDENCE (highest priority after graph fix)
Add `methodology_type` field to graph edges: `theoretical / computational / experimental / physical`. Classify by source URL domain + title keywords. Add `type_diversity` score per invariant. Surface in consolidate.py report and site.

### 2. EXPLICIT RESOLUTION CRITERIA ON OBLIGATIONS
Each new obligation should have a `resolution_criterion` field ("closes when [specific condition]"). Add second Haiku call in `freed.py _phase_obligate()`. Manually backfill O28/O34/O44. Display in site obligation cards as "CLOSES WHEN:".

### 3. RECOVER 109 SKIPPED PHONE PAPERS
`links_queue.json` has 109 entries with `status: "skipped"` — named papers with dead Google Scholar URLs. New script `recover_skipped.py`: extract title from Scholar URL query param → Semantic Scholar title search → if >80% word match, update to `status: "queued"` with real URL. Use `targeted_sweep.py _search_semantic_scholar()` as reference.

### 4. WIRE links_queue.json INTO DAEMON CYCLE
In `freed.py _phase_sweep()`, after targeted/Tamura, drain `MAX_QUEUE_DRAIN=1` entry from curated queue each cycle. Human-submitted papers first, then score-desc. Mark entry `fed_to_daemon` after fetch.

### 5. DAEMON PROPOSES EXPERIMENTS (stretch)
After failed RESOLVE attempt, Haiku generates one concrete actionable experiment for Dave (Python/Claude API, no lab). Store as `ob['experiment_proposal']`, display on site.

---

## What FREED is not

Not a chatbot wrapper. Not a RAG system. Not a knowledge base with vector search.

It is a **self-modifying knowledge organism** — the genome produces its own update components (R[R]=R structurally). Coherence is never 1.000. Open obligations are load-bearing. A scaffold with no open problems is a mirror.
