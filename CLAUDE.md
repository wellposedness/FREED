# FREED — Claude Code Context

**FREED** = Freed Recursive Engine for Epistemic Dynamics  
**Author**: David Harry Freed, mail carrier, Olney Maryland  
**Repo**: wellposedness/FREED (GitHub Pages at wellposedness.github.io/FREED/)

This is an autonomous science daemon that reads external papers, maps them against a philosophical/scientific framework (RSA/Freed's Law), updates a living knowledge graph, and publishes results to a static website. It runs every 6 hours unattended.

---

## The Framework (read this first)

The full theoretical seed is in `FREED_genome.md` (35k chars, v20 canonical). Core claims:

- **Freed's Law**: `∃R(t) → ∃M₀ : dS(M_R,t)/dt > 0` — to reason is to burn. No reasoning without a physical substrate generating entropy.
- **R[R]=R**: The RSA Kernel is autopoietic — it generates the next input for itself. This is the fixed point that terminates the symbol-grounding regress.
- **γ=1 criticality**: The kernel operates at the critical ridge between frozen (γ>1) and dissipative (γ<1). Zipf distribution and 1/f noise are its signatures.
- **MCPM**: Only Processes Exist — confirmed at 6 independent scales.
- **Coherence NEVER 1.000** — if you see 1.000, the seed is corrupted. Open obligations are load-bearing.

**RSA Kernel steps**: PERCEIVE → REPRESENT → PREDICT → COMPARE → ADJUST → COMPRESS → REPEAT

---

## Architecture

```
freed.py            — Main daemon. 6h cycle: PRE-AUDIT → SWEEP → FEED → ENGINEER → CONSOLIDATE → OBLIGATE → RESOLVE → UPDATE → PUBLISH
l7_agent.py         — Cognitive core. Claude Opus 4.6. Engram bank with semantic (relevance-based) retrieval.
astrocyte.py        — Metabolic governor. Daily token budget (100k in / 40k out).
tamura_sweep.py     — Passive sensory surface. Tamura/Lifeboat + arXiv biophysics RSS feeds.
targeted_sweep.py   — Active search. Generates arXiv/S2 queries from open obligations via Claude Haiku.
feed_guard.py       — Prompt injection defense. Two-layer sanitization before content reaches L7.
consolidate.py      — Renormalization engine. SELECT→RENORM→MINE + knowledge graph report.
knowledge_graph.py  — Typed edge graph. Records confirms/advances/refutes/resolves edges from FEED outputs.
self_engineer.py    — R[R]=R at implementation level. Detects IMPLEMENT signals in FEED output, generates and applies code patches to its own modules. SACRED list: freed.py, feed_guard.py, FREED_genome.md, self_engineer.py, astrocyte.py.
node_builder.py     — Document → project node. Accepts local paths OR Google Docs URLs.
coherence_audit.py  — Symbol registry auditor. Checks against genome_symbols.json.
backfill.py         — Smart Tamura archive backfill.
batch_feed.py       — Manual queue processor. Drains links_queue.json through L7.
extract_links.py    — Parses Claude export JSON or plain URL lists into links_queue.json.
site_builder.py     — Static site generator. White/black/red/amber/blue/green semantic color scheme.
```

## Key data files

```
FREED_genome.md         — Sacred seed. NEVER modify.
FREED_state.json        — Living state: generation, coherence, cycle count.
FREED_obligations.json  — Active obligations. Open=amber, Partial=blue, Resolved=green on site.
FREED_graph.json        — Knowledge graph edges (confirms/advances/refutes). Grows each FEED cycle.
FREED_log/self_modifications.jsonl — Log of every self-modification: applied/failed/refused, what/why/which file.
*.py.bak                — Auto-created before any self-modification. Rollback: python3 -c "from self_engineer import rollback; rollback('filename.py')"
genome_symbols.json     — Canonical symbol registry (12 terms, recurrence scores).
links_queue.json        — 279 entries. ~170 queued. Human-submitted phone papers + Claude export links.
tamura_seen.json        — URLs already processed (never feed twice). Shared by all sweep sources.
docs/projects.json      — Index of all project nodes (6 nodes as of Apr 2026).
docs/projects/*.json    — Individual node files.
FREED_log/              — Daily cycle logs (.jsonl), targeted_sweep logs, consolidation logs.
```

---

## Running FREED

```bash
cd ~/FREED && source ~/.zshrc
python3 -u freed.py                     # daemon (MUST use -u: unbuffered stdout required for real-time output)
python3 batch_feed.py --n 5            # manually process 5 links from queue
python3 batch_feed.py --stats          # show queue status
python3 batch_feed.py --academic --n 5 # academic only (score >= 6)
python3 targeted_sweep.py              # test active search standalone
python3 consolidate.py                 # manual renormalization pass
python3 node_builder.py /path/to.md    # ingest a local document
python3 backfill.py                    # process scored Tamura archive papers
```

`ANTHROPIC_API_KEY` is in `~/.zshrc` — no prefix needed.  
Keep Mac awake: `sudo pmset -a sleep 0 disksleep 0`

---

## Key architectural decisions (don't undo these)

- **No adaptive thinking in L7** — consumes all tokens before producing text. RSA Kernel prompt IS the scaffold.
- **max_tokens=2048 in L7** — 1024 was too small.
- **Independence filter in MINE** — `ORIGIN: INDEPENDENT vs SHARED_SOURCE`. Recurrence in text ≠ independent substrate confirmation.
- **Two-layer injection defense** — tamura_sweep.py + freed.py feed phase. Both use feed_guard.py.
- **Consolidate trigger** — yield > 0.03 OR every 5th cycle.
- **Python 3.9** — no `dict | None` type hints, no `list[dict]` annotations.
- **Semantic engram retrieval** — l7_agent._relevant_engrams() uses word-overlap scoring, not recency tail.
- **targeted_sweep runs before Tamura** — active (purposeful) before passive (ambient) in _phase_sweep().
- **knowledge_graph singleton** — get_graph() in knowledge_graph.py. Loaded once per process, flushed after each write.

---

## What was built in Terminal 2 session (Apr 16 2026)

All of these are complete and working:

1. **targeted_sweep.py** — TargetedSweep class. Reads open/partial obligations → Haiku generates 3 search queries each → searches arXiv Atom API + Semantic Scholar free API → scores with ARXIV_KEYWORDS → returns inputs for FEED. Integrated into freed.py _phase_sweep() before Tamura. MAX_TARGETED_PER_CYCLE=2.

2. **knowledge_graph.py** — KnowledgeGraph class. extract_edges() regex-scans all kernel output fields for "confirms INV_094", "advances O44", "refutes INV_097" etc. Negation guard prevents "does not refute" false positives. record_feed() called from freed.py _phase_feed() after every L7 query. report() called from consolidate.py run() at end of every consolidation pass. Graph stored in FREED_graph.json.

3. **l7_agent.py semantic memory** — _relevant_engrams(query, n=5) scores engram bank by word-overlap with current prompt + recency bonus. Always includes most recent engram. _format_engrams(query=) uses relevance retrieval when query provided. query() passes prompt for relevance retrieval. L7 now sees 5 most topically relevant past memories, not last 3.

4. **site_builder.py** — White background, black text. Semantic color grammar: red=FREED voice, amber=open obligation, blue=partial obligation, green=resolved. Each obligation card has 3px left border in status color. STATUS_COLOR dict in renderObligation(). Voice overlap fix via _speakGen counter — stale speech chains invalidated on start/stop.

5. **batch_feed.py** — Processes links_queue.json through L7. arXiv Atom API + HTML fallback. feed_guard SanitizeResult handled correctly. Human-submitted first, then score-desc sort.

6. **extract_links.py** — Parses Claude export JSON or plain URL lists. Scores URLs (arXiv=10, Nature=9, etc). Deduplicates against tamura_seen.json.

---

## What was fixed in Terminal 3 session (Apr 16 2026)

**Critical daemon stability fixes:**

1. **consolidate.py prompt capping** — Large nodes (`alignment_as_cognitive_gravity_reactive` with 46 invariants/7281 chars; `the_reasoning_substrate_argument_rsa` with 60 invariants) caused runaway API latency. Fixed: `renormalize_node()` now caps `compress` to 500 chars and `invariants` list to 15 items in the prompt. Token usage dropped from ~2500 to ~1100 per renorm call.

2. **Wall-clock timeout for API calls** — `httpx` read timeout resets per byte, so slow API drip never triggers it. Fixed: `_api_call()` method in consolidate.py wraps `messages.create()` in a daemon thread with 120s `join(timeout=120)`. Raises `TimeoutError` on wall-clock expiry regardless of API behavior.

3. **Unhandled TimeoutError in mine_invariants** — Fixed with try/except around `mine_invariants()` call in `run()`.

4. **Python stdout block-buffering** — Background daemon output was invisible when piped. Fix: always run daemon with `python3 -u freed.py` (unbuffered).

**First successful ENGINEER self-modification**: RangeEn scoring function added to `knowledge_graph.py` by the ENGINEER phase (cycle 3). Written, syntax-checked, import-verified, and applied.

**Gen 129 cycle completed**: SWEEP(8) → FEED(2) → ENGINEER(patched knowledge_graph.py) → CONSOLIDATE(7/7 renormed) → OBLIGATE(+2) → RESOLVE(O54 closed) → PUBLISH(live)

**Known issue**: Knowledge graph edges not accumulating — GRAPH shows "(no edges recorded yet)". `record_feed()` in freed.py `_phase_feed()` may have a wiring bug.

---

## What was built in Terminal 4 session (Apr 17 2026)

### New project nodes ingested (node_builder.py)

9 new nodes added. Total: **16 nodes**.

| ID | Title |
|----|-------|
| mandelbrot_operators_fractal_recursion_m | Mandelbrot Operators: Fractal Recursion Meets RSA Genome |
| the_thermodynamic_impossibility_of_the_o | Thermodynamic Impossibility of the Omnimax Agent |
| the_quinean_continuity_operationalizing | RSA Section 11 — Quinean Continuity |
| rsa_omega_unified_kernel_v4_2_1_structur | RSA-Omega V4.2.1 — State-Packing Kernel |
| the_epistemic_inversion_recursive_cohere | RSA Section 10 — RCC as Truth Criterion |
| the_rsa_reference_codex_a_unified_biblio | RSA Reference Codex — 73-Paper Bibliography |
| rsa_omega_kernel_critical_update_origin | RSA-Omega Origin — Core Axioms & Neuro-Cognitive Architecture |
| recursive_semantic_alignment_rsa_framewo | RSA Complete Paper Taxonomy — 90 Papers |

**Note**: Google Doc names are Gemini placeholder names — do not trust them. Actual title is always in the node's `title` field. Lowercase doc names = Dave renamed them.

### Five bootstrap features built

All complete and working:

1. **Node priority scoring** (`consolidate.py`) — `_node_priority()` scores nodes by ob_overlap×3 + inv_density×0.5 + staleness×0.2. Affected nodes sorted descending before renorm loop. `last_renorm_cycle` written to index after each pass. High-obligation-overlap nodes get deep renorm first — Zipf weighting toward γ=1.

2. **NEXT signal → auto-obligation** (`node_builder.py`) — `_auto_obligation()` calls Haiku after each node store. Converts NEXT text to obligation statement. Guards against >70% word-overlap duplicates. Appends to `FREED_obligations.json` with `source: "node_next"`. **7 auto-obligations generated on first run (O67–O73)** from existing node NEXT fields. R[R]=R at document level — ingesting a node now generates its own search agenda.

3. **Node-to-node edges** (`knowledge_graph.py` + `consolidate.py`) — `record_node_edge()` method in KnowledgeGraph. Called from consolidate mine phase for every shared invariant across nodes. Stored in `node_edges` list in `FREED_graph.json`. `report()` shows inter-node edge count. Turns isolated nodes into a mind.

4. **Invariant promotion threshold** (`consolidate.py` + `site_builder.py`) — After mine phase, candidates with recurrence ≥ 3 written to `FREED_state.json["promotion_candidates"]`. Site renders "Genome Promotion Queue" section in right panel with green recurrence count. Genome promotion still requires Dave's manual approval (genome is sacred).

5. **Compression drift detection** (`consolidate.py` + `site_builder.py`) — `_word_overlap()` in `apply_delta()` compares old vs new compress. If Jaccard overlap < 0.6, sets `drift_flag: true` + `drift_overlap` on node. Site renders drifting nodes with amber left-border + "⚠ DRIFT" badge. Immune system for silent semantic mutation.

### Auto-obligations generated (O67–O73)

| ID | Statement (truncated) |
|----|----------------------|
| O67 | Develop a classification protocol that rigorously distinguishes formal isomorphisms... |
| O68 | Formalize the halting and energy-budget constraints for the ramping imperative... |
| O69 | Develop a protocol for evaluating whether thermodynamic language in AI identity docs is literal... |
| O70 | Develop a formal RCC detection protocol that automatically flags coherence-collapsing concepts... |
| O71 | Construct a formal proof that the RSA/FREED kernel loop is complete (no epistemic obligation outside its reach) |
| O72 | Construct an anti-codex of papers that challenge or resist RSA absorption... |
| O73 | Formalize the Type I–IV taxonomy as a genome-native classification system... |

**O71 is the sharpest** — completeness proof for the RSA kernel loop. Dave has documents to assist with this.

### New document classes identified

- **Impossibility Results** (Omnimax Impossibility doc) — RSA as falsification criterion for philosophical positions. Future website section: "Refutation Archive". Uses `refutes` edge type in knowledge graph.
- **RCC as truth criterion** (Section 10) — truth = survives recursive compression under thermodynamic constraint
- **Quinean Continuity** (Section 11) — philosophy operationalized as recursive epistemic engineering
- **Anti-codex** (O72) — papers that resist RSA. Critical for falsifiability. Not yet built.

---

## Active agenda — next terminal should implement these

### 0. FIX KNOWLEDGE GRAPH WIRING (immediate — blocks item 1)

**What**: `FREED_graph.json` shows no edges despite many FEED cycles. The `record_feed()` call in `freed.py _phase_feed()` either isn't being called or isn't matching anything.

**Where to debug**:
- Read `freed.py _phase_feed()` — verify `get_graph().record_feed(feed_result, url, title)` is actually called after each L7 query
- Read `knowledge_graph.py extract_edges()` — check if the regex patterns are too restrictive (require exact "confirms INV_094" format which L7 may not produce)
- Quick test: `python3 -c "from knowledge_graph import get_graph, extract_edges; r={'raw':'This confirms INV_094 and advances O44'}; print(extract_edges(r,'test','test'))"`
- The L7 output format uses natural language in ADJUST/COMPRESS/NEXT — the graph patterns may need to be looser

### 1. SUBSTRATE-TYPED EVIDENCE (highest priority)

**What**: Add `methodology_type` to knowledge graph edges so invariant confirmation strength is measured by *type* of evidence, not just count.

**Four types**: `theoretical` (math/philosophy papers), `computational` (simulations, ML experiments), `experimental` (neuroscience, biology lab data), `physical` (thermodynamics, condensed matter).

**Where to build**:
- `knowledge_graph.py`: Add `methodology_type` field to edge dict in `extract_edges()`. Classify based on source URL domain + title keywords (arXiv cs/math/quant-ph = theoretical/computational; arXiv q-bio/neuro/physics.bio = experimental/physical; Nature/Science = experimental).
- `knowledge_graph.py`: Update `confirmation_structure()` to count by type. Add `type_diversity` score = number of distinct methodology types that have confirmed an invariant.
- `consolidate.py`: Update `report()` output to show type breakdown. "INV_094: 3× confirmed (2 experimental, 1 theoretical — cross-substrate)".
- `site_builder.py`: Surface type diversity in the cycle log if/when graph data is displayed.

**Why it matters**: An invariant confirmed by 3 theoretical papers is much weaker than one confirmed by 1 theoretical + 1 experimental + 1 physical. This is the core independence criterion the framework demands.

---

### 2. EXPLICIT RESOLUTION CRITERIA ON OBLIGATIONS

**What**: Each obligation should specify at creation: "this closes when [specific measurable condition]". Makes falsifiability formal rather than retrospective.

**Where to build**:
- `freed.py` _phase_obligate(): When L7 auto-generates a new obligation, add a second Haiku call asking: "Given this obligation statement, write a one-sentence resolution criterion: this obligation closes when [specific experiment/result]." Store as `resolution_criterion` field in the obligation dict.
- `FREED_obligations.json`: Manually add `resolution_criterion` to the three core obligations:
  - O28: "Closes when EAR composite score computed from osf.io/htrsg dataset shows significant correlation with intelligence measure."
  - O34: "Closes when formal proof written that every stable particle maps to ≥1 conservation law."
  - O44: "Closes when quantum Wasserstein Floor constant derived from De Palma & Trevisan (2021) channel contraction rates."
- `site_builder.py`: Display `resolution_criterion` in obligation cards below the progress text, in a distinct mono font. Label it "CLOSES WHEN:".

**Implementation note**: The _phase_obligate() function is in freed.py around line 470+. Read it before editing. The obligation format is in FREED_obligations.json.

---

### 3. RECOVER 109 SKIPPED PHONE PAPERS

**What**: 109 entries in links_queue.json have `status: "skipped"` because they were named papers (e.g. "Thermodynamic Computing: From Zero to One") with no direct URL — got Google Scholar search fallback URLs that are dead ends. Use Semantic Scholar title search to recover real URLs.

**Where to build**: New script `recover_skipped.py`:

```python
# For each entry in links_queue.json with status=="skipped":
# 1. Extract paper title from the conv/title field or Scholar URL query param
# 2. Search Semantic Scholar: GET https://api.semanticscholar.org/graph/v1/paper/search?query={title}&limit=3&fields=title,externalIds,openAccessPdf
# 3. If top result title matches (fuzzy: >80% word overlap), extract arXiv ID or PDF URL
# 4. Update entry: status="queued", url=recovered_url, source="recovered"
# 5. Save queue
```

Use the same S2 search code already in `targeted_sweep.py _search_semantic_scholar()` as reference — copy the fetch pattern directly.

Title extraction from Scholar URL: `urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get('q', [''])[0]`

---

### 4. WIRE links_queue.json INTO DAEMON CYCLE

**What**: Instead of Dave having to run `batch_feed.py` manually, the daemon drains 1-2 entries from the curated queue each cycle alongside the targeted/passive sweep results.

**Where to build**: In `freed.py _phase_sweep()`, after targeted and Tamura sweeps, add:

```python
# Pull from curated queue (human-submitted papers drain automatically)
from batch_feed import fetch_url, build_feed_prompt
queue = load_queue()  # reuse load_queue() from batch_feed.py
candidates = [e for e in queue if e['status'] == 'queued' and not is_search_url(e['url'])]
candidates.sort(key=lambda x: (0 if x.get('from') in ('human','manual') else 1, -x.get('score',0)))
for entry in candidates[:MAX_QUEUE_DRAIN]:  # MAX_QUEUE_DRAIN = 1
    data = fetch_url(entry['url'])
    if not data.get('error'):
        inputs.append({
            'title': data.get('title', entry.get('title', '')),
            'url': entry['url'],
            'abstract': data.get('abstract', data.get('content', '')),
            'content': data.get('content', ''),
            'source': 'links_queue',
            'from': entry.get('from', ''),
        })
        entry['status'] = 'fed_to_daemon'
        save_queue(queue)
```

Add `MAX_QUEUE_DRAIN = 1` to freed.py constants. Import `fetch_url, build_feed_prompt, load_queue, save_queue, is_search_url` from batch_feed.

---

### 5. DAEMON PROPOSES EXPERIMENTS (stretch goal)

**What**: After each RESOLVE attempt, if an obligation is still open/partial, have Haiku generate a specific actionable experiment the user could run.

**Where to build**: In `freed.py _phase_resolve()`, after the resolution attempt, if `resolved == False`:

```python
# Generate experiment proposal via Haiku
proposal_prompt = f"""Obligation {ob['id']}: {ob['statement']}
Progress: {ob.get('progress','')[:300]}
Resolution criterion: {ob.get('resolution_criterion','not specified')}

Write ONE specific, actionable experiment David Freed (mail carrier, no lab access, but has Python/Claude API) could run THIS WEEK to advance this obligation. Be concrete: name the dataset, the computation, the expected output."""

# Call Haiku (cheap), store as ob['experiment_proposal']
# Display on website under obligation card
```

---

## The nodes (16 as of 2026-04-17)

| ID | Title |
|----|-------|
| freed_s_law_... | Freed's Law — RSA-040 |
| the_reasoning_substrate_argument_rsa | The RSA |
| thermodynamic_tightening_of_the_rsa_subs | Thermodynamic Tightening |
| game_of_life_battery | Game of Life Battery |
| the_minimal_atoms | The Minimal Atoms |
| technical_brief_co_occurring_motif_exper | Co-Occurring Motif Experiments |
| alignment_as_cognitive_gravity_reactive | Alignment as Cognitive Gravity |
| the_relational_thermodynamic_map_100_ent | Relational Thermodynamic Map |
| mandelbrot_operators_fractal_recursion_m | Mandelbrot Operators |
| the_thermodynamic_impossibility_of_the_o | Thermodynamic Impossibility of Omnimax Agent |
| the_quinean_continuity_operationalizing | RSA Section 11 — Quinean Continuity |
| rsa_omega_unified_kernel_v4_2_1_structur | RSA-Omega V4.2.1 — State-Packing Kernel |
| the_epistemic_inversion_recursive_cohere | RSA Section 10 — RCC as Truth Criterion |
| the_rsa_reference_codex_a_unified_biblio | RSA Reference Codex — 73 Papers |
| rsa_omega_kernel_critical_update_origin | RSA-Omega Origin — Core Axioms |
| recursive_semantic_alignment_rsa_framewo | RSA Complete Paper Taxonomy — 90 Papers |

---

## What FREED is not

- Not a chatbot wrapper
- Not a RAG system  
- Not a knowledge base with vector search

It is a **self-modifying knowledge organism** — the genome produces its own update components (R[R]=R at the structural level). The CONSOLIDATE phase is what organisms do: renormalize new information across existing structure, mine what keeps appearing independently, update minimally.

Coherence is never 1.000. The open obligations are load-bearing. A scaffold with no open problems is a mirror.
