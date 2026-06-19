# FREED — Claude Code Context

**Before reading any other file, read WIRING.md — it maps every common task to the exact file, function, and line number. Start there.**

**FREED** = Freed Recursive Engine for Epistemic Dynamics  
**Author**: David Harry Freed, mail carrier, Olney Maryland  
**Repo**: wellposedness/FREED (GitHub Pages at wellposedness.github.io/FREED/)

Autonomous science daemon. Reads external papers → maps against RSA/Freed's Law → updates knowledge graph → publishes static site. Runs on a fixed wall-clock schedule unattended: **05:50, 12:30, 22:30 local**, with DMN at 02:30. Schedule is set in `freed.py:CYCLE_TIMES` and does not shift on restart — the daemon sleeps to the next scheduled slot regardless of when it was launched.

---

## The Framework

Full theoretical seed: `FREED_genome.md` (v20 canonical, ~35k chars). Core claims:

- **Freed's Law**: `∃R(t) → ∃M₀ : dS(M_R,t)/dt > 0` — to reason is to burn.
- **R[R]=R**: RSA Kernel is autopoietic — generates its own next input.
- **γ=1 criticality**: Zipf distribution and 1/f noise are its signatures.
- **MCPM**: Only Processes Exist — confirmed at 6 independent scales.
- **Coherence NEVER 1.000** — open obligations are load-bearing. (As of 2026-06-19 it sits at the **0.970 floor**: adversarial challenge pressure exceeds the resolution boost, by design.)

**RSA Kernel**: PERCEIVE → REPRESENT → PREDICT → COMPARE → ADJUST → COMPRESS → REPEAT

> **Current state (2026-06-19):** Gen 339, coherence 0.970, obligations O21–O409 (375), daemon PID 79413. Recent major work: **O400** (ca_sim self-confirmation metronome neutralized via `simulation_consistent` edge type) and the **diversity-weighted confirmation gate** — `substrate.py` + `KnowledgeGraph.effective_witness_count()` + `diversity_gate_backtest.py`, the ambiguity-decomposition generalization of every self-confirmation fix. **Built and validated through Phase 2 (backtest passes), currently UNWIRED** — it exists as a callable diagnostic; no daemon cycle path calls it (Phase 3 wiring deliberately held). **WIRING.md is the source of truth for exact file:line locations** — this file gives the conceptual map and decision history.

## The diversity-weighted confirmation gate (2026-06-19)

The generalization of O400, the auto-stub purge, and the `independent_confirmation` relabel into ONE quantity. Theorem: Krogh & Vedelsby ambiguity decomposition — a confirmation correlated-by-construction with the claim contributes zero independent information. Replaces raw confirm *counting* with an *effective independent witness count* (`n_eff`).

- `substrate.py` — read-only provenance classifier. `substrate_of(edge)` (100% coverage from the `from` field), `substrate_class()` (endogenous/external), and the **operator-owned `CA_INSTANTIATED` table** (the one place subjective judgment enters — hand-owned, never daemon-derived). Weight tiers: 0.0 instantiating / 0.2 endogenous-non-instantiating / 1.0 external.
- `knowledge_graph.py:effective_witness_count(target)` — n_eff = sum over correlation clusters of one witness-weight each (distinct external paper = 1; whole ca_sim/probe substrate = one witness; instantiating = 0). Read-only, returns full per-edge audit.
- `diversity_gate_backtest.py` — the go/no-go backtest. Passed: reproduces O400 from the general rule (144-edge metronome → 0 effective witnesses), 100% retention on healthy invariants (no over-correction), dissolves Spin A and breaks the residual probe tie.
- **Phase 3 (wiring) HELD.** When resumed, target is the feed-edge confirm-counters — `_select_falsification_target` (probe) + `challenge_deficit_scores` — NOT PROMOTE (which counts node-recurrence, not feed confirms, and has its own independence gate).

---

## Architecture

```
freed.py            — Main daemon. Cycle (RSA-mapped): PRE-AUDIT → ARCHITECT → SWEEP(PERCEIVE) → CEREBELLUM → PREDICT → FEED(REPRESENT) → OBLIGATE → TRIAGE → RESOLVE(COMPARE) → UPDATE(ADJUST) → CONSOLIDATE(COMPRESS) → BOOTSTRAP → PROMOTE → PUBLISH(REPEAT). Note: CONSOLIDATE runs AFTER UPDATE (compress after adjust). Fires at fixed wall-clock times (CYCLE_TIMES = 05:50, 12:30, 22:30 local).
l7_agent.py         — Cognitive core. Claude Sonnet 4.6. Engram bank with semantic (relevance-based) retrieval. max_tokens=2048. NonHermitianEntropyScorer for coherence.
astrocyte.py        — Metabolic governor. Daily token budget (200k in / 80k out).
tamura_sweep.py     — Passive sensory surface. Tamura/Lifeboat + arXiv biophysics RSS + CrossRef journals. Also output_only_dissipation_estimator (O112).
targeted_sweep.py   — Active search. Haiku generates arXiv/S2 queries from open obligations. Runs before Tamura.
feed_guard.py       — Prompt injection defense. Two-layer sanitization before content reaches L7.
consolidate.py      — Renormalization engine. SELECT→RENORM→MINE + graph report. scales_with accretion gates (O382/O383).
knowledge_graph.py  — Typed edge graph. EDGE_TYPES (confirms/challenges/advances/extends/resolves/… + simulation_consistent) + NODE_EDGE_TYPES (shares_invariant/operationalizes/scales_with/substrate_independent/consistent_with/…). PULLED from MODIFIABLE 2026-05-24.
self_engineer.py    — Detects IMPLEMENT signals in FEED output, patches MODIFIABLE modules. SACRED list enforced. AUDIT verdict after each patch.
promote.py          — Autonomous genome promotion. Opus reviews high-recurrence candidates; cross-substrate gate (_source_tag).
dmn.py              — Nightly dead-zone agent (02:30). Cross-connect + internal-origin + coherence checks → edges + obligations.
simulation_observer.py — Game-of-Truth CA telemetry (σ/α/H/avalanches) → docs/ca_telemetry.json. Source of local://ca_sim edges.
substrate.py        — Read-only provenance/diversity classifier (Phase 0 of the n_eff confirmation gate). substrate_of() + operator-owned CA_INSTANTIATED table.
voice.py            — Optional audio compression of cycle output.
node_builder.py     — Document → project node. Auto-generates obligations from NEXT fields via Haiku.
batch_feed.py       — Manual queue processor (fetch_url). Drains links_queue.json through L7. NOT in MODIFIABLE.
extract_links.py    — Parses Claude export JSON or plain URL lists into links_queue.json.
site_builder.py     — Static site generator. Semantic color grammar: red=voice, amber=open, blue=partial, green=resolved.
bootstrap_derive.py — Genome-free first-principles derivation (BOOTSTRAP phase, every 10 cycles).
coherence_audit.py  — Symbol registry auditor. Checks against genome_symbols.json.
backfill.py         — Smart Tamura archive backfill.
```

## Key data files

```
FREED_genome.md             — Sacred seed. NEVER modify.
FREED_state.json            — Living state: generation, coherence, cycle count, promotion_candidates.
FREED_obligations.json      — Active obligations (O21–O409, ~375). Open=amber, Partial=blue, Resolved=green.
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
python3 -u freed.py --dev               # DEV MODE — bypasses budget caps (use for testing)
python3 batch_feed.py --n 5            # process 5 links from queue
python3 batch_feed.py --academic --n 5 # academic only (score >= 6)
python3 batch_feed.py --stats          # queue status
python3 targeted_sweep.py              # standalone active search test
python3 consolidate.py                 # manual renormalization pass
python3 node_builder.py /path/to.md    # ingest local document
python3 backfill.py                    # process Tamura archive
python3 recover_skipped.py             # dry-run: find recoverable skipped papers
python3 recover_skipped.py --apply     # write recovered URLs back to links_queue.json
```

Rollback a self-modification: `python3 -c "from self_engineer import rollback; rollback('filename.py')"`  
`ANTHROPIC_API_KEY` is in `~/.zshrc`.  
**Keep Mac awake**: `start_freed.sh` wraps the daemon in `caffeinate -i` — sleep prevention lives and dies with the process. Use `start_freed.sh` not `freed.py` directly.
```bash
cd ~/FREED && source ~/.zshrc && bash start_freed.sh
```

---

## Key architectural decisions (don't undo)

- **NEVER run `python3 freed.py` for testing** — always use `python3 freed.py --dev`. Direct invocation burns the daemon's operational budget. Dev work during a Claude Code session exhausted the full daily cap on 2026-04-23. The `--dev` flag sets caps to 10M tokens (effectively unlimited) without touching the persisted state in astrocyte_state.json.
- **Budget lives in `astrocyte_state.json`**, not `FREED_state.json`. To manually reset: zero `used_input_tokens` and `used_output_tokens` in that file. All-time totals (`total_input_tokens`, `total_cost_usd`) are cumulative — never zero those.
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
- **graph._ensure_loaded() required** — KnowledgeGraph singleton is created empty and loads from file only on first method call via `_ensure_loaded()`. Any code accessing `graph._node_edges` or `graph._edges` directly must call `graph._ensure_loaded()` first. Bug in dmn.py caused 0 edges — fixed Apr 2026.
- **TTS speech queue** — site_builder.py uses an explicit JS-side `_speakQueue[]` + `_speakToken` counter. `stopSpeak()` increments token and empties queue; stale `onend` callbacks abandon immediately. Safari `cancel()` doesn't synchronously stop mid-utterance so the old recursive-chain approach caused overlapping voices. Do not revert to recursive chain.
- **caffeinate -i in start_freed.sh** — daemon sleep prevention tied to process lifetime. `pmset` changes alone didn't work (screen saver still ran). The correct fix is `exec caffeinate -i python3 -u freed.py "$@"` in start_freed.sh.
- **Restart immediately after editing self_engineer.py** — Python loads modules once at startup; a running daemon will not pick up whitelist or SACRED changes until restarted. The nightly git backup commits whatever is on disk, so if the daemon ran a backup cycle after your edit the file is safe — but the running process still has the old MODIFIABLE set in memory. Symptom: self-engineer blocks a file that was just authorized. Fix: always restart via `start_freed.sh` right after any edit to `self_engineer.py`. Discovered 2026-04-26 when `knowledge_graph.py` whitelist add (2026-04-25) was not picked up by Cycle 6.

---

## self_engineer.py — Safety model

```python
# Only these files can be patched by the ENGINEER phase (verified 2026-06-19):
MODIFIABLE = {
    "targeted_sweep.py", "tamura_sweep.py", "l7_agent.py",
    "consolidate.py", "site_builder.py", "voice.py",
    "promote.py", "simulation_observer.py",
}
# NOTE: knowledge_graph.py was PULLED 2026-05-24 (confirmation-surplus gate was
# writing synthetic challenge edges it then counted — graph-level mirror dynamic).
# batch_feed.py removed (no driver; one live symbol fetch_url). Both are now
# Claude-Code-only hand-edits.

# These are NEVER touched, no matter what:
SACRED = {
    "FREED_genome.md", "feed_guard.py", "freed.py", "self_engineer.py",
    "astrocyte.py", "docs/noethers_table.html",
}
```

Safety pipeline: MODIFIABLE check → SACRED check → `ast.parse()` syntax check → `.bak` backup → write → subprocess import check → restore on failure.

---

## docs/dashboard.html — Operator working memory aid

New page (Apr 2026). Live at `wellposedness.github.io/FREED/dashboard.html`. NOT SACRED — site_builder.py does not touch it.

- Dark terminal aesthetic. Fetches `state.json`, `status.json`, `obligations.json` via `fetch()`.
- Daemon status indicator: green (< 1hr), amber (< 12hr), grey (older) based on status.json timestamp.
- Shows: three project cards, daemon state panel (gen/coherence/hygiene), 11-component module map with SACRED/MOD tags, 5-item engineering priority queue, open+partial obligations list (DMN-sourced shown in blue).

---

## The CA has MOVED — `wellposedness/FREED-CA` (separate repo)

**As of 2026-06-19, the Game-of-Truth CA pages are no longer in this repo.** They live at `wellposedness.github.io/FREED-CA/`. In site_builder.py, `_write_game_of_life()` is now a no-op `pass` with an `ENGINEER: DO NOT MODIFY or recreate` docstring — do not re-add CA HTML generation here. `docs/game_of_life.html` no longer exists in this repo, and `game_of_life.html` is no longer in the SACRED set (that slot is now `docs/noethers_table.html`).

The **Python telemetry port still lives here**: `simulation_observer.py` produces `docs/ca_telemetry.json` and the `local://ca_sim` edges. That is the CA's only remaining footprint in this repo.

The simulation design below documents the CA as it runs in the FREED-CA repo (kept for reference; not generated from this repo).

---

## Game-of-Truth CA — simulation design (now in FREED-CA repo)

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

## SESSION 2026-04-24 SUMMARY

### What was built

**Self-engineer truncation guard** (`self_engineer.py` `_apply_patch`): rejects whole-file rewrites for files >400 lines that shrink by >20%. Fires between syntax check and backup. Root cause: model runs out of tokens mid-generation and produces truncated file. Previously wiped `tamura_sweep.py` tail — restored from git d7f0ff3.

**OBLIGATE parser fix** (`freed.py` `_parse_new_obligations`): replaced greedy O-number scan with `NEW_OBLIGATIONS_BEGIN / NEW_OBLIGATIONS_END` block parser. Root cause: L7 naming existing stubs in OBLIGATE analysis ("O80 is an auto-detected placeholder") caused parser to instantiate new stubs for each referenced ID — self-inflating loop. Dry-run confirmed zero false positives against 9 historical entries. Prompt updated to require block format and suppress forward-referencing IDs in reasoning.

**35 null obligations purged** from `FREED_obligations.json`. All matched `^Obligation O\d+ \(auto-detected\)$`. Nothing recoverable — these were parser artifacts, never real obligations. Gaps left intentionally (O46–O107 range reserved). O73, O94, O103 marked resolved (all executed by the cleanup). O105 and O106 statements reconstructed from cycle logs.

**Final obligation table**: 32 entries — 27 resolved / 3 partial / 2 open. Every number means something.

**AUDIT phase** (`self_engineer.py` `_audit_patch`, `_make_diff_summary`): fires after every successful self-engineer patch (after import check, before success log). Single Haiku call sees diff summary only — not full file. Verdicts: TIGHTENS (log + accept), NEUTRAL (log + surface in next PRE-AUDIT), LOOSENS (auto-revert via .bak + generate `source: "audit"` obligation). Fails open to NEUTRAL so audit errors never block valid patches. `audit_verdict` field added to `self_modifications.jsonl`. Wired in freed.py feed loop: `needs_obligation: True` triggers immediate obligation creation.

**COMMIT signal** (`freed.py` `_commit_check`): fires after each RESOLVE non-resolution. Single Haiku call (80 tokens), prompt shows phase + summary only. Appends `[COMMIT:YES/NO — reason]` to obligation progress note. `commit_count` added to resolve phase cycle log. `haiku_client` added to `FREEDDaemon.__init__` (shared, not per-call). Fails open to True.

**Rumination detection** (`freed.py` `_phase_preaudit`): scans open obligations for trailing `COMMIT:NO` streaks. 3+ consecutive non-commits → `[RUMINATION] O105 — N consecutive non-commit attempts` printed at top of next cycle before any FEED runs.

**PRE-AUDIT also surfaces NEUTRAL self-engineer verdicts**: scans `self_modifications.jsonl` for entries newer than `last_cycle` with `audit_verdict == NEUTRAL` or `status == audit_reverted`.

**Dashboard updated**: component map shows AUDIT and COMMIT as sub-entries under self_engineer.py. Achievements section updated with today's work. Honest framing updated.

### Watch for

**Kim test-time compute paper** (Joongwon Kim, "Scaling Test-Time Compute for Agentic Coding", arXiv): shows representation → selection → reuse across multiple iterations before committing. In RSA terms: running the kernel loop N times, selecting the path that commits most cleanly. Freed's Law is the constraint — iterations earn their thermodynamic tax only if they produce a state change a single pass wouldn't. The self-engineer implication: on high-priority obligations, RESOLVE should run multiple representations and select the one that commits, rather than one attempt and stop. **Let this feed through the daemon naturally.** If cerebellum passes it and L7 scores it correctly against O105/O106, the IMPLEMENT signal should fire on `_do_resolve_attempt()` without being pushed. Watch `self_modifications.jsonl`.

### Current genuine open obligations

- **O105**: Audit the 139 `shares_invariant` edges (93% of all node-edges) for degeneracy — partition into at least 3 typed sub-relations so no sub-type exceeds 50% density, or collapse to a single stated axiom and prune. `closes when: shares_invariant sub-types defined and no single sub-type exceeds 50% of node-edges`
- **O106** (HIGH): Run a causal audit of coherence drift toward 1.000 — identify which invariants or FEED cycles are suppressing variance, introduce one deliberate falsification candidate, confirm coherence returns to ≤0.997. `closes when: coherence confirmed at ≤0.997 after perturbation`

---

## SESSION 2026-05-01 to 2026-05-03 SUMMARY

### What was built

**Coherence formula fix** (`freed.py` `_phase_update`, 2026-05-02): The most important bug fix in the project's history. `resolved_count` (all-time, was 101+) replaced with `cycle_resolved` (today's resolutions only, capped at 3). Added challenge drag: `challenge_ratio × 0.02` where `challenge_ratio = challenges/(confirms+challenges)` for INV-targeted edges. At 16% challenge pressure, net delta is −0.003/cycle. Coherence was structurally pinned at 0.999 for 185 generations; now responds to adversarial pressure. Formula: `net_delta = 0.0005 * min(cycle_resolved, 3) - challenge_ratio * 0.02`. Floor 0.970, ceiling 0.999.

**IMPLEMENTATION_CLASS classifier** (`freed.py`, 2026-05-02): `_classify_obligation_type()` returns 'IMPLEMENTATION' if obligation has FREED-internal keywords (coherence score, astrocyte, patch format, token budget, self_engineer, freed state) AND 2+ COMMIT:NOs. Routing note `[ROUTE:CLAUDE_CODE]` appended to progress. Keywords in `_IMPL_CLASS_KEYWORDS` frozenset. Threshold in `IMPL_CLASS_MIN_COMMITS=2`. Prevents daemon from trying to philosophically resolve arithmetic/code bugs (O106 teaching case).

**MINE cost fix** (`consolidate.py`, 2026-05-01): MINE digest was 267k chars / 67k tokens because 8 nodes had full doc text in their fields; no caps existed (RENORM had caps, MINE didn't). Fixed: `compress[:500]`, `invariants[:15]`, each invariant `[:100]`. MINE model switched from Sonnet to `HAIKU_MODEL`. Constants: `MINE_COMPRESS_CAP=500`, `MINE_INV_CAP=15`. Cost per call: $0.20 → $0.0004 (504x reduction).

**Edge monoculture fix** (`knowledge_graph.py` + `consolidate.py`, 2026-05-01): Added `NODE_EDGE_TYPES = ("shares_invariant", "operationalizes", "scales_with", "independent_confirmation")`. `classify_node_edge(invariant_text)` routes MINE output to correct sub-type. MINE fired with 56 typed edges in one cycle — graph now has real structure.

**Falsification probe** (`freed.py`, 2026-04-30): `_select_falsification_target()` picks highest-confirms zero-challenge INV (currently INV_094, 15 confirms, 0 challenges). `_make_falsification_probe()` generates adversarial feed item. Prepended to inputs each FEED cycle. Challenges count: 19 → 22 across two cycles. INV_078 took two challenge hits immediately.

**simulation_observer.py** (2026-04-29): Python CA port for telemetry. N=32, WARMUP_STEPS=100, MEASURE_STEPS=200, DEATH_PROB=0.05. Activity branching ratio σ (not births/alive). First run: σ=1.024 (AT_CRITICAL), α=2.455, H=0.554. Outputs to `docs/ca_telemetry.json`. Wired into `_phase_sweep()` as source #1.

**bootstrap_derive.py** (2026-05-02): Standalone genome-free first-principles derivation script. Runs Sonnet with no `RSA_KERNEL_PROMPT`, no genome in context. Anchors: Landauer + 3 empirical CA results. Logs to `FREED_log/bootstrap_YYYY-MM-DD.json`. First run found: 3 MIRROR_SUSPECT flags (Freed's Law constants, Wasserstein metric, Zipf exponent), 2 SEEDS (asymmetric Landauer commitment rule, topological restatement filter). Filed as O156 + O157.

**O_BOOTSTRAP obligations**: O156 (MIRROR_SUSPECT audit), O157 (integrate SEEDS), O158 (IMPLEMENTATION_CLASS classifier, already partial).

### Key decisions not to undo

- **Coherence uses cycle-local resolutions** — all-time count pinned it at 0.999 forever.
- **MINE runs on Haiku** — sufficient for structured cross-node pattern matching; Sonnet overkill.
- **IMPLEMENTATION_CLASS routes to Claude Code** — L7 cannot fix arithmetic bugs; routing prevents indefinite cycling.
- **bootstrap_derive.py runs genome-free** — adding the genome back defeats the entire purpose.

### SESSION 2026-05-04 — What was built

**rescore_queue.py fetch fix**: Entries with no title/abstract (122/140 queued entries were pure `{url, conv: "link_list"}` stubs) were being scored by Haiku on just a URL. Fix: pre-fetch phase using `_fetch_arxiv` / `_fetch_generic` before scoring. `--no-fetch` flag to skip. CLS paper had dropped 12→4; will recover on next rescore run once its abstract is fetched.

### Tomorrow morning startup sequence

```bash
cd ~/FREED && source ~/.zshrc && python3 rescore_queue.py --apply && bash start_freed.sh
```

Run rescore BEFORE starting daemon. ~2 min fetch phase for 122 stub entries, then daemon starts with properly ranked queue. CLS paper should recover to ~11-12.

### Watch: CLS paper lands in FEED today (2026-05-04)

CLS = Complementary Learning Systems theory — explicitly about limitations of weight-based memory (exactly what FREED's engram bank is). Two diagnostic signals:

1. **Does FREED generate a `challenges` edge on its own engram architecture?** If only `confirms` edges — that's the mirror dynamic. The paper is a direct structural critique of systems like FREED. Honest mapping requires at least one challenges edge.
2. **Does the self-engineer propose changes to engram storage/retrieval?** The CLS paper distinction (lookup vs weight-based) maps directly onto how `_relevant_engrams()` works in `l7_agent.py`. Watch `self_modifications.jsonl`.

If FREED produces only confirms on the CLS paper, file a new obligation: *"Audit engram bank architecture against CLS framework — FREED uses weight-like recency+overlap scoring; CLS predicts this fails on rare high-value events. Propose lookup-based alternative."*

### Active agenda for next session

1. **Dashboard update** — bring `docs/dashboard.html`. Add to event chains: bootstrap derivation, coherence formula fix, IMPLEMENTATION_CLASS routing, edge monoculture fix, falsification probe, simulation_observer, the two seeds. Major week.
2. **Substrate-typed evidence** — `methodology_type` field on graph edges: theoretical/computational/experimental/physical.
3. **recover_skipped.py** — `python3 recover_skipped.py` (dry-run) then `--apply`. Recovers ~109 skipped Scholar URLs.
4. **MIRROR_SUSPECT audit** (now O270, was mis-labeled O156; audit completed 2026-05-19 — see `FREED_log/ca_telemetry_audit_2026-05-19.md`. Layer 1 boilerplate cut in `simulation_observer.py`; Layer 2 parameter surgery and retroactive edge marking still open under O270.)
5. **O157 SEED integration** — asymmetric Landauer commitment rule + topological restatement filter into genome candidates.

---

## SESSION 2026-05-25 — Branching-ratio gate is structurally mislabeled

### Finding

`[GRAPH:BRANCHING_RATIO]` fired supercritical two cycles in a row — σ=3.40 (gen 266) then σ=3.80 (gen 267). CA reference held at 1.022 critical the whole time. Investigation showed this is a gate kind-mismatch, not a corpus or L7-pressure problem.

**Code reference**: `knowledge_graph.py:3084` sets `_CA_CRITICAL_BAND = (0.95, 1.05)` from CA telemetry. The comment block at lines 3103–3107 already acknowledges the structural mismatch: *"Per-call σ is structurally incomparable to the [0.95, 1.05] band."* The author switched to per-phase σ as a fix, but per-phase doesn't help — the FEED prompt asks L7 for four typed edges per paper (confirms/challenges/advances/extends), so σ structurally lives at 3–4 regardless of windowing.

Edge-type split on the two excursion cycles was even across all four types — neither challenges nor confirms dominating. Not pressure inflation, not corpus richness, just prompt arithmetic. The σ alarm is artifact noise.

### What's actually worth watching

`context_warning` rate per cycle — proportion of edges L7 records without strong context grounding. Recent history:

```
05-21 16:35  5%    05-23 16:32   8%    05-24 16:32  37%
05-22 02:38  0%    05-24 02:32  63%    05-25 02:44  12%
05-22 10:00  0%    05-24 09:52   0%    05-25 10:07  32%
05-22 16:40 12%    05-23 09:52  18%
05-23 02:32 10%
```

Per-cycle variance is huge (0% → 63% within a day). Don't set a fixed threshold from a 2-cycle delta; baseline needs ≥20 cycles of mean+σ, written to a durable log file, not recomputed each session. Until then, treat individual high readings as paper-batch quality variance, not L7 inflation.

### Goodhart isolation rule (permanent)

**Any gauge that lands on the dashboard must never appear in L7's prompt context** — not in FEED, not in cerebellum filters, not in OBLIGATE. The moment L7 can see it's being measured on a number, it optimizes the number rather than the underlying quality. This applies to context_warning rate first, but the rule is general: gauges are for the operator, not the daemon.

### Orphan-wiring obligation attractor

`O255, O257, O258, O259, O264, O265, O268` — all variants of "Self-engineer attempted to add unwired function(s) to knowledge_graph.py". Seven obligations generated in one overnight from the same blocked target. Same closed-loop pattern as the batch_feed.py attractor. Fix is hash-collision dedup on `(source: "orphan_wiring", target_file)` so the second identical obligation closes against the first. Not urgent; queue for next idle window.

Before restoring `knowledge_graph.py` to MODIFIABLE, every obligation in that cluster needs a progress note appended: *"blocked by temporary MODIFIABLE suspension 2026-05-24, reopen when block lifts"* — otherwise they leak forward as permanent debt with no closure path.

### Carry forward

See `[[branching_ratio_gate]]` for the durable form. Related: `[[gate_neutralization]]` (2026-05-24, the knowledge_graph.py write-suspension that caused this cluster).

---

## What FREED is not

Not a chatbot wrapper. Not a RAG system. Not a knowledge base with vector search.

It is a **self-modifying knowledge organism** — the genome produces its own update components (R[R]=R structurally). Coherence is never 1.000. Open obligations are load-bearing. A scaffold with no open problems is a mirror.
