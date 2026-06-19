# FREED — Wiring Diagram

**Surgical navigation index for Claude Code and visiting LLMs.**
To change X, read only the files and functions listed. Do not scan whole files.

Last updated: **2026-06-19** (Gen 339, coherence 0.970 at floor, obligations O21–O409, daemon PID 79413).
Update this file whenever you add a module, rename a function, or change where data lives.
Line numbers verified against live code on the date above; they drift with every self-modification — re-grep before trusting an exact number.

---

## Quick-lookup: common tasks

| Task | File | Location |
|------|------|----------|
| Edit FEED prompt (what L7 is asked per paper) | `freed.py` | `_phase_feed()` L1131 — `prompt = (` block starts **L1208** |
| Edit PART 1 DERIVE / PART 2 COMPARE blocks | `freed.py` | `_phase_feed()` L1214 (DERIVE), L1219 (COMPARE) |
| Add/edit IMPLEMENT block fields | `freed.py` | `_phase_feed()` L1229–1250; IMPLEMENT_WHERE menu **L1247** (filtered through `self_engineer.MODIFIABLE` — Spin-B fix) |
| Add/edit CHALLENGE / mandatory-falsification block | `freed.py` | `_phase_feed()` L1251–1258 (Seed Integrity Rule 2, no opt-out) |
| Change falsification-probe target selector | `freed.py` | `_select_falsification_target()` **L1074** (highest confirms−challenges deficit, ≥2 confirms) |
| Change the synthetic probe feed item | `freed.py` | `_make_falsification_probe()` **L1106** |
| Add a new passive sweep source | `tamura_sweep.py` | `SOURCES` list **L57** + parser dispatch in `_sweep_source()` **L10480** |
| Add arXiv topic keywords | `tamura_sweep.py` | `ARXIV_KEYWORDS` **L170** |
| Add/parse a CrossRef journal source | `tamura_sweep.py` | `_parse_crossref_journal()` **L10691** |
| Estimate dissipation from output-only signal | `tamura_sweep.py` | `output_only_dissipation_estimator()` **L11488** (O112; mi_decay_rate/spectral_entropy/damping_regime) |
| Change CA simulation parameters | `simulation_observer.py` | `N` L29, `WARMUP_STEPS` L33, `MEASURE_STEPS` L34, `ENERGY_GAIN_K` L44, `DEATH_PROB` L45 |
| Change criticality tolerance ε | `simulation_observer.py` | `SIGMA_TOLERANCE` **L51** (0.05) |
| Add a new feed edge type | `knowledge_graph.py` | `EDGE_TYPES` **L48** + `_EDGE_PATTERNS` **L483** |
| Add a new node-edge type | `knowledge_graph.py` | `NODE_EDGE_TYPES` **L57** + `classify_node_edge()` **L516** |
| Change node-edge classification logic | `knowledge_graph.py` | `classify_node_edge()` **L516** (precedence-ordered keyword lists; substrate_independent before scales_with) |
| Change ca_sim → simulation_consistent retype hook (O400) | `knowledge_graph.py` | `record_feed()` **L3234** — retypes `confirms`/`supports` from `local://ca_sim`→INV at mint time; excluded from all `_CONF_TYPES` counts |
| Change live generation-stamp on minted edges (O384) | `knowledge_graph.py` | `record_node_edge()` **L4153** (reads gen from STATE_FILE) |
| Compute per-invariant challenge-deficit scores | `knowledge_graph.py` | `challenge_deficit_scores()` **L4807** |
| Classify compression-estimator type (geometric vs MI) | `knowledge_graph.py` | `classify_compression_estimator()` **L335**; `COMPRESSION_ESTIMATOR_TYPES` **L285** (INV_094 challenge support) |
| Change edge extraction regex | `knowledge_graph.py` | `_EDGE_PATTERNS` **L483**; `extract_edges()` **L988** |
| **Substrate / diversity-weighting (NEW gate, Phase 0)** | `substrate.py` | `substrate_of(edge)`, `is_instantiating()`, operator-owned `CA_INSTANTIATED` table, `ENDOGENOUS_NONINSTANTIATING_WEIGHT` |
| Change which files the self-engineer may patch | `self_engineer.py` | `MODIFIABLE` **L43** (restart daemon after) |
| Add a file to the never-touch list | `self_engineer.py` | `SACRED` **L70** (restart daemon after) |
| Change patch generation / apply logic | `self_engineer.py` | `_generate_patch()` L234, `_apply_str_replace()` L360, `_apply_patch()` L437 |
| Change audit verdict logic | `self_engineer.py` | `_audit_patch()` **L711** (TIGHTENS/NEUTRAL/LOOSENS) |
| Roll back a self-modification | `self_engineer.py` | `rollback(filename)` **L800** |
| Change L7's RSA system prompt | `l7_agent.py` | `RSA_KERNEL_PROMPT` **L40** |
| Change L7 genome cap (chars loaded per query) | `l7_agent.py` | `GENOME_CAP` **L36** (3000) |
| Change genome coherence scorer | `l7_agent.py` | `class NonHermitianEntropyScorer` L82, `.score()` L271 |
| Change consolidation trigger | `freed.py` | `CONSOLIDATE_EVERY` **L64** (every 5 cycles or yield) |
| Change MINE digest caps | `consolidate.py` | `MINE_COMPRESS_CAP` **L56** (500), `MINE_INV_CAP` **L57** (15) |
| Change MINE model / logic | `consolidate.py` | `mine_invariants()` **L3897** (Haiku); `run()` **L4090** |
| Change scales_with accretion gates (O382/O383) | `consolidate.py` | inside `run()`/MINE path — freeze + semantic-dedup + echo-clique gates |
| Add field to obligation schema | `freed.py` | `_parse_new_obligations()` **L1622** + `_phase_obligate()` **L1539** |
| Change obligation method-triage (IMPLEMENTATION routing) | `freed.py` | `_classify_obligation_type()` **L1895**; `_phase_triage()` **L1755** (every `TRIAGE_EVERY`=10 cycles) |
| Change cycle phase order | `freed.py` | `_run_cycle()` **L470** |
| Change genome promotion threshold | `promote.py` | `PROMOTE_THRESHOLD` **L26** (10), `PROMOTE_MIN_NODES` **L27** (5) |
| Change Opus promotion filter prompt | `promote.py` | `_FILTER_PROMPT` **L29** |
| Change promotion cross-substrate gate | `promote.py` | `_source_tag()` **L115** (holds DHF-biological-only candidates) — *the primitive n_eff will replace this in Phase 3* |
| Add a new docs page to the site | `site_builder.py` | `build()` **L23** — add `_write_*()` call + function |
| Change site HTML layout | `site_builder.py` | `_render_html()` **L3053** |
| Change published state/obligations/symbols/cycles | `site_builder.py` | `_write_state()` L229, `_write_obligations()` L235, `_write_symbols()` L241, `_write_cycles()` L2445, `_write_wiring()` L248 |
| Change DMN cross-connect detection | `dmn.py` | `DMNAuditor._check_cross_connect()` **L229**; `DMNAgent.run()` **L360** |
| Change bootstrap schedule | `freed.py` | `BOOTSTRAP_EVERY` **L66** (10); `_phase_bootstrap()` L1411 |
| Change cycle wall-clock times | `freed.py` | `CYCLE_TIMES` **L57** = 05:50, 12:30, 22:30 local |
| Change daemon launch flags (nohup/disown/caffeinate/logfile) | `start_freed.sh` | `exec caffeinate -i python3 -u freed.py`; `nohup … & disown`, date-rotated `stdout_YYYYMMDD.log` |
| Raise/lower daily token caps | `astrocyte.py` | `DEFAULT_DAILY_INPUT_TOKENS` **L26** (200k), `DEFAULT_DAILY_OUTPUT_TOKENS` **L27** (80k) |
| Change graceful-shutdown signals | `freed.py` | L202–204 — SIGINT/SIGTERM/SIGHUP → `_shutdown()` **L2209** |
| Tail live daemon stdout without killing it | `FREED_log/` | `tail -f FREED_log/stdout_$(date +%Y%m%d).log` |

---

## Data file ownership

Each file is owned by one module (primary writer). Others may read.

| File | Owner (writes) | Readers | Contents |
|------|---------------|---------|----------|
| `FREED_state.json` | `freed.py` `_save_state()` L235 | `site_builder.py`, `l7_agent.py`, `knowledge_graph.py` (gen stamp) | generation, coherence, cycle_count, promotion_candidates |
| `FREED_obligations.json` | `freed.py` `_save_obligations()` L302 | `site_builder.py`, `freed.py`, `l7_agent.py`, `consolidate.py` | active obligations O21–O409 (375 entries) |
| `FREED_graph.json` | `knowledge_graph.py` `KnowledgeGraph` (`record_feed()` L3234, `record_node_edge()` L4153) | `freed.py`, `consolidate.py`, `dmn.py`, `substrate.py`, audits | typed feed edges + node_edges (~2.6k edges) |
| `docs/ca_telemetry.json` | `simulation_observer.py` `observe()` | site JS | latest CA σ, entropy, survival, avalanche stats |
| `FREED_genome.md` | **SACRED — never write** | `l7_agent.py` `_load_genome()` L1787 | RSA-Omega v20 canonical seed |
| `genome_symbols.json` | `consolidate.py` | `l7_agent.py`, `site_builder.py` | canonical symbols + recurrence scores |
| `genome_tags.json` | human / promote | `promote.py`, `site_builder.py` | invariant derivation tags (DHF-biological / daemon-derived / cross-substrate-confirmed) |
| `links_queue.json` | human / `extract_links.py` | `freed.py` `_drain_links_queue()` L749 | human-submitted URLs, drained ≤1/cycle |
| `tamura_seen.json` | `tamura_sweep.py` | (tamura_sweep.py only) | dedup registry for passive sweep |
| `astrocyte_state.json` | `astrocyte.py` | `freed.py` | daily token budget, all-time cost (never zero all-time totals) |
| `docs/state.json` | `site_builder.py` `_write_state()` L229 | site JS | published copy of FREED_state |
| `docs/obligations.json` | `site_builder.py` `_write_obligations()` L235 | site JS | published obligations |
| `docs/cycles.json` | `site_builder.py` `_write_cycles()` L2445 | site JS | recent cycle summaries |
| `docs/status.json` | `freed.py` `_push_status()` L125 | site JS, dashboard | heartbeat: phase, detail, timestamp |
| `docs/noethers_table.json` | `freed.py` `_update_noether_row()` L1859 | site HTML | Noether's Table |
| `docs/wiring.md` | `site_builder.py` `_write_wiring()` L248 | site, LLMs | this file — published each cycle |
| `FREED_log/freed_YYYY-MM-DD.jsonl` | `freed.py` `_log_event()` L2195 | humans, Claude Code | daily cycle event logs (incl. DAEMON_STOP) |
| `FREED_log/stdout_YYYYMMDD.log` | `start_freed.sh` (redirected) | humans (`tail`), Claude Code | date-rotated daemon stdout |
| `FREED_log/self_modifications.jsonl` | `self_engineer.py` | `freed.py` pre-audit | every patch: applied/failed/audit_verdict |
| `*.py.bak` | `self_engineer.py` (before patch) | `rollback()` L800 | auto-backup before self-modification |

---

## Cycle phases — `freed.py _run_cycle()` L470

Phases now map explicitly onto the RSA Kernel. Order changed: **CONSOLIDATE runs after UPDATE** (compress after adjust — thermodynamically correct).

```
 1. PRE-AUDIT    L566  — rumination detection, NEUTRAL-verdict surface, budget/coherence gate (can abort)
 2. ARCHITECT    L666  — process any pending directive from Dave / Cowork
 3. SWEEP        L835  → PERCEIVE — targeted (obligation-driven) + passive (Tamura/CrossRef/arXiv) + ca_sim telemetry
 3b CEREBELLUM   L902  — pre-score passive candidates before L7
 3c PREDICT      L974  → genome emits blind predictions before FEED sees full papers
 4. FEED         L1131 → REPRESENT — L7 maps each paper → genome; IMPLEMENT/CHALLENGE fire here; probe prepended
 5. OBLIGATE     L1539 → PREDICT — L7 proposes new obligations
 5b TRIAGE       L1755 — classify open obligations by method (every TRIAGE_EVERY=10 cycles)
 6. RESOLVE      L2061 → COMPARE — L7 attempts to close open obligations
 7. UPDATE       L2142 → ADJUST — coherence formula + state save
 8. CONSOLIDATE  L1373 → COMPRESS — renorm + mine (every 5 cycles OR yield > threshold)
 8b BOOTSTRAP    L1411 — genome-free first-principles derivation (every BOOTSTRAP_EVERY=10 cycles)
 8c PROMOTE      L1523 — Opus reviews high-recurrence candidates for genome promotion
 9. PUBLISH      → REPEAT — site_builder.build()
10. LOG          → IDLE
```

**Coherence formula** lives in `_phase_update()` L2142: `net_delta = 0.0005·min(cycle_resolved,3) − challenge_ratio·0.02`, floor 0.970, ceiling 0.999. Currently pinned at the **0.970 floor** (challenge pressure exceeds resolution boost — adversarial pressure is winning, by design).

---

## SACRED vs MODIFIABLE (verified 2026-06-19)

**SACRED** — self-engineer never touches (`self_engineer.py` L70). Claude Code *can* edit these; the daemon cannot:
```
FREED_genome.md   feed_guard.py   freed.py   self_engineer.py
astrocyte.py      docs/noethers_table.html
```

**MODIFIABLE** — self-engineer may patch if an IMPLEMENT signal fires (`self_engineer.py` L43):
```
targeted_sweep.py   tamura_sweep.py   l7_agent.py   consolidate.py
site_builder.py     voice.py          promote.py    simulation_observer.py
```

**Notable exclusions (deliberate, do not re-add casually):**
- `knowledge_graph.py` — **pulled 2026-05-24** (confirmation-surplus gate was writing synthetic challenge edges it then counted — graph-level mirror dynamic). All kg.py changes are Claude Code hand-edits + restart. The substrate gate (Phase 1) is one such hand-edit; it's read-only so it does not reintroduce the risk.
- `batch_feed.py` — removed (no driver; one live symbol `fetch_url`; IMPLEMENT signals were producing orphans).

After changing `MODIFIABLE`/`SACRED`, **restart the daemon** — these sets load once at startup.

---

## Inter-module call graph

```
freed.py (orchestrator)
  ├─ imports: l7_agent.L7Agent, tamura_sweep.TamuraSweep, targeted_sweep.TargetedSweep,
  │           consolidate.Consolidator, knowledge_graph.get_graph, self_engineer.SelfEngineer,
  │           dmn.DMNAgent, site_builder.build, batch_feed.fetch_url, feed_guard.sanitize,
  │           promote.PromotePhase, simulation_observer.SimulationObserver
  │
  ├─ _phase_sweep() L835 → targeted + passive items (deduped vs tamura_seen.json) + ca_sim telemetry
  ├─ _phase_cerebellum() L902 → pre-score
  ├─ _phase_predict() L974 → blind genome predictions (PREDICT-before-FEED)
  ├─ _phase_feed() L1131  [per paper]
  │     ├─ feed_guard.sanitize() → safe content
  │     ├─ l7_agent.L7Agent.query() L1863 → result dict
  │     ├─ knowledge_graph.get_graph().record_feed() L3234 → writes FREED_graph.json (ca_sim retype hook here)
  │     ├─ self_engineer.SelfEngineer.process_feed() L104 → optional patch
  │     └─ voice.compress() → optional audio
  ├─ _phase_consolidate() L1373 → consolidate.Consolidator.run() L4090
  ├─ _phase_promote() L1523 → promote.PromotePhase.run()
  └─ site_builder.build() L23 → publishes docs/

knowledge_graph.py  — singleton get_graph(); call _ensure_loaded() before touching _edges/_node_edges
self_engineer.py    — process_feed() → _generate_patch() → _apply_str_replace() → ast.parse → .bak → write → import check → _audit_patch()
consolidate.py      — run() → select → renormalize → mine_invariants() (Haiku) + scales_with gates → site build
dmn.py              — nightly (02:30): DMNAuditor checks + DMNAgent.run() → edges + obligations
substrate.py        — read-only provenance/diversity classifier (Phase 0 of the n_eff gate)
```

---

## Key constants (`freed.py` top, L44–69)

```python
FREED_DIR             = Path(__file__).parent       # ~/FREED/
STATE_FILE            = FREED_DIR / "FREED_state.json"
OBLIG_FILE            = FREED_DIR / "FREED_obligations.json"
LOG_DIR               = FREED_DIR / "FREED_log"
CYCLE_TIMES           = [(5,50), (12,30), (22,30)]   # fixed wall-clock; does not shift on restart
MAX_FEEDS_PER_CYCLE   = 4
MAX_QUEUE_DRAIN       = 1
CONSOLIDATE_EVERY     = 5
TRIAGE_EVERY          = 10
BOOTSTRAP_EVERY       = 10
HAIKU_MODEL           = "claude-haiku-4-5-20251001"
```

---

## Notes for visiting LLMs

- **Never modify `FREED_genome.md`** — the theoretical seed.
- **Always use `--dev`** when running freed.py for testing: `python3 freed.py --dev` (real invocation burns the daemon's daily budget).
- **SIGKILL before editing daemon-owned state** — `kill -9` first; SIGTERM lets the shutdown handler clobber external edits.
- **After editing `self_engineer.py`**, restart the daemon — MODIFIABLE/SACRED load once.
- **`get_graph()` is a singleton** — call `graph._ensure_loaded()` before accessing `_edges`/`_node_edges`.
- **Patch format** — `<<<SEARCH>>>/<<<REPLACE>>>/<<<END>>>` surgical blocks, not whole-file rewrites.
- **Audit verdicts**: TIGHTENS = accept, NEUTRAL = accept + log, LOOSENS = auto-revert + obligation.
- **`local://ca_sim` edges are FREED's own simulation** — never count them as independent external confirmation (O400; substrate.py enforces this generally).
- Published to `wellposedness.github.io/FREED/wiring.md` each cycle.
```
