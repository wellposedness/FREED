# FREED — Wiring Diagram

**Surgical navigation index for Claude Code and visiting LLMs.**
To change X, read only the files and functions listed. Do not scan whole files.

Last updated: 2026-04-26. Update this file whenever you add a module, rename a function, or change where data lives.

---

## Quick-lookup: common tasks

| Task | File | Location |
|------|------|----------|
| Edit FEED prompt (what L7 is asked per paper) | `freed.py` | `_phase_feed()` L821 — `prompt = (...)` block L847–874 |
| Add/edit IMPLEMENT block fields | `freed.py` | `_phase_feed()` L862–866 — also update IMPLEMENT_WHERE list |
| Add/edit CHALLENGE / falsification block | `freed.py` | `_phase_feed()` L867–874 |
| Add a new passive sweep source | `tamura_sweep.py` | `SOURCES` list L57 + `parsers` dict inside `_sweep_source()` L530 |
| Change CA simulation parameters | `simulation_observer.py` | `N`, `WARMUP_STEPS`, `MEASURE_STEPS`, `DEATH_PROB`, `ENERGY_GAIN_K` at top of file |
| Change criticality threshold ε | `simulation_observer.py` | `SIGMA_TOLERANCE` at top of file |
| Add arXiv topic keywords | `tamura_sweep.py` | `ARXIV_KEYWORDS` L170 |
| Add CrossRef journal source | `tamura_sweep.py` | `_CROSSREF_HEADERS` L731, `_parse_crossref_journal()` L738 |
| Add a new edge type to the graph | `knowledge_graph.py` | `EDGE_TYPES` L46 + `_EDGE_PATTERNS` L126 |
| Change edge extraction regex | `knowledge_graph.py` | `_EDGE_PATTERNS` L126 |
| Change what files the self-engineer may patch | `self_engineer.py` | `MODIFIABLE` L43 (restart daemon after) |
| Add a file to the never-touch list | `self_engineer.py` | `SACRED` L55 (restart daemon after) |
| Change patch format / str_replace logic | `self_engineer.py` | `_generate_patch()` L199, `_apply_str_replace()` L287 |
| Change audit verdict logic | `self_engineer.py` | `_audit_patch()` L517 |
| Change L7's RSA system prompt | `l7_agent.py` | `RSA_KERNEL_PROMPT` L40 |
| Change L7 genome cap (chars loaded per query) | `l7_agent.py` | `GENOME_CAP` L36 |
| Change consolidation trigger threshold | `freed.py` | `CONSOLIDATE_EVERY` L57, `YIELD_THRESHOLD` (search in file) |
| Add field to obligation schema | `freed.py` | `_parse_new_obligations()` L1068 + `_phase_obligate()` L991 |
| Change cycle phase order | `freed.py` | `_run_cycle()` L438 |
| Add a new docs page to the published site | `site_builder.py` | `build()` L23 — add `_write_*()` call + function |
| Change site HTML layout | `site_builder.py` | `_render_html()` L198 |
| Change how genome symbols are published | `site_builder.py` | `_write_symbols()` L76 |
| Change DMN's cross-connect detection | `dmn.py` | `DMNAuditor._check_cross_connect()` L229 |
| Change genome promotion threshold | `promote.py` | `PROMOTE_THRESHOLD` L22, `PROMOTE_MIN_NODES` L23 |
| Change Opus promotion filter prompt | `promote.py` | `_FILTER_PROMPT` L26 |
| Revert a genome promotion | `FREED_genome.md` | Delete the `## [PROMOTED YYYY-MM-DD]` block at end of file |
| Change how FREED's genome coherence is scored | `l7_agent.py` | `NonHermitianEntropyScorer.score()` L266 |
| Change pre-audit rumination detection | `freed.py` | `_phase_preaudit()` L520 |
| Update Noether's Table row from FEED | `freed.py` | `_maybe_update_noether_row()` L1292 |

---

## Data file ownership

Each file is owned by one module (primary writer). Others may read.

| File | Owner (writes) | Readers | Contents |
|------|---------------|---------|----------|
| `FREED_state.json` | `freed.py` `_save_state()` L191 | `site_builder.py`, `l7_agent.py` | generation, coherence, cycle_count, promotion_candidates |
| `FREED_obligations.json` | `freed.py` `_save_obligations()` L258 | `site_builder.py`, `freed.py`, `l7_agent.py`, `consolidate.py` | active obligations O28–O141+ |
| `docs/ca_telemetry.json` | `simulation_observer.py` `SimulationObserver.observe()` | site JS (future) | latest CA σ, entropy, survival, avalanche stats |
| `FREED_graph.json` | `knowledge_graph.py` `KnowledgeGraph` | `freed.py`, `consolidate.py`, `dmn.py` | typed edges (confirms/refutes/challenges/…) + node_edges |
| `FREED_genome.md` | **SACRED — never write** | `l7_agent.py` `_load_genome()` L426 | RSA-Omega v20 canonical seed |
| `genome_symbols.json` | `consolidate.py` | `l7_agent.py`, `site_builder.py` | 12 canonical symbols + recurrence scores |
| `links_queue.json` | human / `extract_links.py` | `freed.py` `_drain_links_queue()` L685 | human-submitted URLs, drained 1/cycle |
| `tamura_seen.json` | `tamura_sweep.py` | (tamura_sweep.py only) | dedup registry for passive sweep |
| `astrocyte_state.json` | `astrocyte.py` | `freed.py` | daily token budget, all-time cost |
| `docs/state.json` | `site_builder.py` `_write_state()` L64 | site JS | published copy of FREED_state |
| `docs/obligations.json` | `site_builder.py` `_write_obligations()` L70 | site JS | published obligations |
| `docs/symbols.json` | `site_builder.py` `_write_symbols()` L76 | site JS | published genome_symbols |
| `docs/cycles.json` | `site_builder.py` `_write_cycles()` L101 | site JS | last 50 cycle summaries |
| `docs/status.json` | `freed.py` `_push_status()` L84 | site JS, dashboard | heartbeat: phase, timestamp |
| `docs/noethers_table.json` | `freed.py` `_update_noether_row()` L1305 | site HTML | Noether's Table (also hand-curated HTML) |
| `docs/wiring.md` | `site_builder.py` `_write_wiring()` | site, LLMs | this file — published each cycle |
| `FREED_log/freed_YYYY-MM-DD.jsonl` | `freed.py` `_log_event()` L1547 | humans, Claude Code | daily cycle event logs |
| `FREED_log/self_modifications.jsonl` | `self_engineer.py` | `freed.py` pre-audit | every patch: applied/failed/audit_verdict |
| `*.py.bak` | `self_engineer.py` (before patch) | `self_engineer.rollback()` L606 | auto-backup before self-modification |

---

## Inter-module call graph

```
freed.py (orchestrator)
  ├─ imports: l7_agent.L7Agent, tamura_sweep.TamuraSweep, targeted_sweep.TargetedSweep
  │           consolidate.Consolidator, knowledge_graph.get_graph, self_engineer.SelfEngineer
  │           dmn.DMNAgent, site_builder.build, batch_feed.fetch_url, feed_guard.sanitize
  │
  ├─ _phase_sweep() L743
  │     ├─ targeted_sweep.TargetedSweep.sweep(obligations) → targeted items
  │     └─ tamura_sweep.TamuraSweep.sweep() → passive items (deduped against tamura_seen.json)
  │
  ├─ _phase_feed() L821  [per paper]
  │     ├─ feed_guard.sanitize(content) → safe_content
  │     ├─ l7_agent.L7Agent.query(prompt) → result dict
  │     ├─ knowledge_graph.get_graph().record_feed(result, ...) → writes FREED_graph.json
  │     ├─ self_engineer.SelfEngineer.process_feed(result, ...) → optional patch
  │     └─ voice.compress(text) → optional audio
  │
  ├─ _phase_consolidate() L953
  │     └─ consolidate.Consolidator.run(new_knowledge, ...) → renorm + mine + site build
  │
  ├─ _phase_obligate() L991
  │     └─ l7_agent.L7Agent.query(obligate_prompt) → new obligations
  │
  ├─ _phase_resolve() L1440
  │     └─ l7_agent.L7Agent.query(resolve_prompt) → progress notes
  │
  └─ site_builder.build(state, obligations, cycle_log) → publishes docs/

l7_agent.py (cognitive core)
  ├─ reads: FREED_genome.md (first 3000 chars), FREED_obligations.json, engram bank
  └─ writes: engram bank (internal log)

knowledge_graph.py (graph store)
  ├─ singleton: get_graph() — load-on-first-call, flush after write
  ├─ extract_edges(kernel_output) → parse EDGE_PATTERNS from L7 text
  └─ reads/writes: FREED_graph.json

self_engineer.py (patcher)
  ├─ process_feed() → detect IMPLEMENT signal → _generate_patch() → _apply_str_replace()
  │                 → ast.parse() check → .bak backup → write → import check
  └─ _audit_patch() → Haiku verdict (TIGHTENS/NEUTRAL/LOOSENS) → revert if LOOSENS

consolidate.py
  ├─ imports: knowledge_graph.get_graph, site_builder.build, astrocyte.Astrocyte
  └─ run() → select_affected() → renormalize_node() → mine_invariants() → site build

dmn.py (nightly dead-zone agent)
  ├─ DMNAuditor: cross-connect + internal-origin + genome-coherence checks
  └─ DMNAgent.run(graph, obligations, state) → edges + new obligations
```

---

## SACRED vs MODIFIABLE

**SACRED** — self-engineer will never touch these (enforced in `self_engineer.py` L55):
```
FREED_genome.md   feed_guard.py   freed.py   self_engineer.py
astrocyte.py      docs/game_of_life.html
```
Claude Code **can** edit SACRED files. The daemon cannot.

**MODIFIABLE** — self-engineer may patch these if IMPLEMENT signal fires (`self_engineer.py` L43):
```
targeted_sweep.py   tamura_sweep.py   l7_agent.py   consolidate.py
knowledge_graph.py  site_builder.py   batch_feed.py  voice.py
promote.py
```

After changing `MODIFIABLE` or `SACRED`, **restart the daemon** — Python loads these sets once at startup. The running process will not see the edit until restart.

---

## Cycle phases (freed.py `_run_cycle()` L438)

```
PRE-AUDIT   L520  — rumination detection, NEUTRAL verdict surface
ARCHITECT   L606  — (reserved, currently light)
SWEEP       L743  — targeted (obligation-driven) + passive (Tamura/CrossRef/arXiv)
CEREBELLUM  L802  — (scoring/routing)
FEED        L821  — L7 maps each paper → genome; IMPLEMENT/CHALLENGE signals fire here
CONSOLIDATE L953  — renorm + mine (every 5 cycles OR yield > threshold)
OBLIGATE    L991  — L7 proposes new obligations
RESOLVE     L1440 — L7 attempts to close open obligations
UPDATE      L1512 — state save, site publish
```

---

## Key constants (freed.py L40–60 area)

```python
FREED_DIR         = Path(__file__).parent          # ~/FREED/
STATE_FILE        = FREED_DIR / "FREED_state.json"
OBLIG_FILE        = FREED_DIR / "FREED_obligations.json"
LOG_DIR           = FREED_DIR / "FREED_log"
CONSOLIDATE_EVERY = 5   # also consolidate every N cycles regardless of yield
MAX_QUEUE_DRAIN   = 1   # human-submitted links drained per cycle (links_queue.json)
```

---

## Notes for visiting LLMs

- **Do not modify `FREED_genome.md`** — it is the theoretical seed. Changes here break the whole organism.
- **Always use `--dev` flag** when running freed.py for testing: `python3 freed.py --dev`
- **After editing `self_engineer.py`**, restart the daemon — the MODIFIABLE/SACRED sets are loaded once.
- **`knowledge_graph.get_graph()` is a singleton** — call `graph._ensure_loaded()` before accessing `_edges` or `_node_edges` directly.
- **Patch format** — self-engineer uses `<<<SEARCH>>>/<<<REPLACE>>>/<<<END>>>` surgical blocks, not whole-file rewrites.
- **Audit verdicts**: TIGHTENS = accept, NEUTRAL = accept + log, LOOSENS = auto-revert + create obligation.
- **`links_queue.json`** — append URLs here to queue papers for the next cycle's FEED. Dave does this manually.
- This wiring diagram is published to `wellposedness.github.io/FREED/wiring.md` each cycle.
