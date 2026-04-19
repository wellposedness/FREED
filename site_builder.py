"""
FREED — Site Builder
Generates a static website into docs/ after every cycle.
GitHub Pages serves docs/ — the world sees FREED working in real time.

The site is the daemon explaining itself.
It does not describe FREED. It IS FREED's output surface.
"""

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

FREED_DIR  = Path(__file__).parent
DOCS_DIR   = FREED_DIR / "docs"
DOCS_DIR.mkdir(exist_ok=True)

CYCLES_LOG = DOCS_DIR / "cycles.json"
MAX_CYCLES = 50   # keep last 50 cycles in public log


def build(state: dict, obligations: list, cycle_log: dict = None):
    """
    Called by freed.py after every UPDATE phase.
    Writes state.json, obligations.json, cycles.json, index.html, then pushes to GitHub.
    """
    _write_state(state)
    _write_obligations(obligations)
    _write_cycles(cycle_log)
    _write_symbols()
    _write_index()
    _write_game_of_life()
    print("[SITE] docs/ updated.")
    _push(state.get("generation", "?"))


# ── Git push ─────────────────────────────────────────────────────────────────

def _push(generation):
    """Stage changed files and push to GitHub. Silent on nothing-to-push."""
    try:
        subprocess.run(
            ["git", "add", "docs/", "FREED_state.json", "FREED_obligations.json"],
            cwd=FREED_DIR, check=True, capture_output=True,
        )
        result = subprocess.run(
            ["git", "commit", "-m", f"Gen {generation} — FREED cycle"],
            cwd=FREED_DIR, capture_output=True, text=True,
        )
        if "nothing to commit" in result.stdout:
            print("[SITE] Nothing new to push.")
            return
        subprocess.run(
            ["git", "push"],
            cwd=FREED_DIR, check=True, capture_output=True,
        )
        print(f"[SITE] Pushed gen {generation} to GitHub Pages.")
    except subprocess.CalledProcessError as e:
        print(f"[SITE] Push failed: {e.stderr or e}")


# ── Data files ────────────────────────────────────────────────────────────────

def _write_state(state: dict):
    out = dict(state)
    out["generated"] = datetime.now(timezone.utc).isoformat()
    (DOCS_DIR / "state.json").write_text(json.dumps(out, indent=2))


def _write_obligations(obligations: list):
    (DOCS_DIR / "obligations.json").write_text(
        json.dumps(obligations, indent=2, ensure_ascii=False)
    )


def _write_symbols():
    """Copy genome_symbols.json to docs/ for browser access."""
    src = FREED_DIR / "genome_symbols.json"
    if src.exists():
        (DOCS_DIR / "symbols.json").write_text(src.read_text(encoding="utf-8"))


def _write_game_of_life():
    """Write the RSA-Omega Game of Truth simulation page. Does not overwrite existing edits."""
    if (DOCS_DIR / "game_of_life.html").exists():
        return
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Freed's Law Simulation — FREED</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;1,300;1,400&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:      #ffffff;
    --surface: #f7f7f5;
    --border:  #e0ddd8;
    --accent:  #b91c1c;
    --green:   #16a34a;
    --amber:   #b45309;
    --text:    #111111;
    --muted:   #374151;
    --mono:    'JetBrains Mono','Fira Code','Courier New',monospace;
    --serif:   'Cormorant Garamond','Palatino Linotype',Georgia,serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg); color: var(--text);
    font-family: var(--serif); font-size: 15px; line-height: 1.7;
    padding: 1.5rem 1.5rem;
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  .container { max-width: 960px; margin: 0 auto; }

  /* Header */
  .header { border-bottom: 1px solid var(--border); padding-bottom: 0.9rem; margin-bottom: 1.4rem; }
  .header .nav { font-family: var(--mono); font-size: 0.72rem; color: var(--muted); margin-bottom: 0.5rem; letter-spacing: 0.06em; }
  .header h1 { font-family: var(--serif); font-weight: 300; font-size: 1.9rem; color: var(--accent); letter-spacing: 0.02em; }
  .header .sub { font-family: var(--serif); font-weight: 300; font-style: italic; color: var(--muted); font-size: 1rem; margin-top: 0.2rem; }

  /* Canvas display */
  .sim-wrap {
    width: 100%; background: #080808;
    border: 1px solid var(--border); margin-bottom: 0.9rem; line-height: 0;
  }
  canvas { display: block; width: 100%; height: auto; image-rendering: pixelated; }

  /* Controls */
  .controls {
    display: flex; flex-wrap: wrap; gap: 0.45rem;
    align-items: center; margin-bottom: 0.9rem;
  }
  .btn {
    padding: 0.35rem 0.85rem; background: transparent;
    border: 1px solid var(--accent); color: var(--accent);
    font-family: var(--mono); font-size: 0.72rem; letter-spacing: 0.07em;
    cursor: pointer; transition: background 0.12s, color 0.12s;
  }
  .btn:hover, .btn.active { background: var(--accent); color: var(--bg); }
  .speed-wrap { display: flex; align-items: center; gap: 0.45rem; font-family: var(--mono); font-size: 0.68rem; color: var(--muted); }
  input[type=range] { accent-color: var(--accent); width: 80px; }

  /* Stats — flush rows matching home page state panel */
  .stats { margin-bottom: 1.4rem; }
  .stat-row {
    display: flex; justify-content: space-between; align-items: baseline;
    padding: 0.28rem 0; border-bottom: 1px solid var(--border);
  }
  .stat-row:last-child { border-bottom: none; }
  .stat-row .label { font-family: var(--mono); color: var(--muted); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.08em; }
  .stat-row .value { font-family: var(--mono); font-size: 0.82rem; color: var(--text); }
  .stat-row .value.accent { color: var(--accent); font-weight: 600; }
  .stat-row .value.green  { color: var(--green);  font-weight: 600; }

  /* Sections */
  .section { margin-bottom: 1.6rem; }
  .section-title {
    font-family: var(--mono); font-size: 0.72rem; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.12em; color: var(--text);
    border-bottom: 1px solid var(--border); padding-bottom: 0.35rem; margin-bottom: 0.8rem;
  }
  .theory p { font-family: var(--serif); font-weight: 300; font-size: 1rem; margin-bottom: 0.7rem; color: var(--text); }
  .theory em { font-style: italic; }
  .theory .law {
    font-family: var(--mono); font-size: 0.88rem;
    background: var(--surface); border-left: 3px solid var(--accent);
    padding: 0.7rem 1rem; margin: 0.9rem 0; line-height: 1.6;
  }
  .theory .law .sub-law { font-family: var(--serif); font-weight: 300; font-style: italic; font-size: 0.9rem; color: var(--muted); margin-top: 0.3rem; }
  .legend { display: flex; gap: 1.4rem; flex-wrap: wrap; margin-top: 0.5rem; }
  .legend-item { display: flex; align-items: center; gap: 0.4rem; font-family: var(--mono); font-size: 0.68rem; color: var(--muted); }
  .legend-swatch { width: 13px; height: 13px; border-radius: 2px; flex-shrink: 0; }
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <div class="nav"><a href="index.html">← FREED</a></div>
    <h1>Freed's Law — The Game of Truth</h1>
    <div class="sub">Each cell is an agent. The hidden physics is Mandelbrot. The view zooms into the boundary. Survival requires modeling it.</div>
  </div>

  <div class="controls">
    <button class="btn" id="btn-play" onclick="togglePlay()">▶ PLAY</button>
    <button class="btn" onclick="stepOnce()">STEP</button>
    <button class="btn" onclick="resetSim()">RESET</button>
    <div class="speed-wrap">
      SPEED <input type="range" id="speed" min="1" max="20" value="8">
    </div>
  </div>

  <div class="sim-wrap">
    <canvas id="canvas"></canvas>
  </div>

  <div class="stats">
    <div class="stat-row"><span class="label">Generation</span><span class="value accent" id="stat-gen">0</span></div>
    <div class="stat-row"><span class="label">Alive Cells</span><span class="value" id="stat-alive">—</span></div>
    <div class="stat-row"><span class="label">Avg Energy</span><span class="value" id="stat-energy">—</span></div>
    <div class="stat-row"><span class="label">Avg Error <span style="font-size:0.6rem;opacity:0.7">(break-even 0.50)</span></span><span class="value" id="stat-error">—</span></div>
    <div class="stat-row"><span class="label">Avg Complexity <span style="font-size:0.6rem;opacity:0.7">(L1 weight norm)</span></span><span class="value" id="stat-complex">—</span></div>
    <div class="stat-row"><span class="label">Boundary cells <span style="font-size:0.6rem;opacity:0.7">(near fractal edge — harder to predict)</span></span><span class="value" id="stat-boundary">—</span></div>
    <div class="stat-row"><span class="label">Zoom level</span><span class="value" id="stat-zoom">0 (1×)</span></div>
    <div class="stat-row"><span class="label">Survival rate this level</span><span class="value" id="stat-survival">100%</span></div>
    <div class="stat-row"><span class="label">Compression ratio <span style="font-size:0.6rem;opacity:0.7">(survivors L1 / level-start L1)</span></span><span class="value" id="stat-compress-r">—</span></div>
    <div class="stat-row"><span class="label">Avg error at level start</span><span class="value" id="stat-err-start">—</span></div>
    <div class="stat-row"><span class="label">Reproductions</span><span class="value" id="stat-repro">0</span></div>
    <div class="stat-row"><span class="label">Deaths</span><span class="value" id="stat-deaths">0</span></div>
  </div>

  <div class="section">
    <div class="section-title">Color Legend — Error × Complexity</div>
    <div class="legend">
      <div class="legend-item"><div class="legend-swatch" style="background:#080808;border:1px solid #333"></div>Dead — no model</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#7f1d1d"></div>Deep red — overfitting (high error, high complexity)</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#ea580c"></div>Orange — underfitting (high error, low complexity)</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#ca8a04"></div>Yellow — exploring (converging, not yet stable)</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#16a34a"></div>Green — accurate but expensive (low error, high complexity)</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#06b6d4"></div>Cyan — compression attractor (low error, low complexity)</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#f8fafc;border:1px solid #ccc"></div>White — reproducing</div>
    </div>
  </div>

  <div class="section theory">
    <div class="section-title">What This Is</div>
    <p>
      A cellular automaton where each cell carries a private 3×3 weight matrix —
      a local generative model of the universe. The hidden physics is the Mandelbrot set:
      z<sub>n+1</sub> = z²<sub>n</sub> + c. Each cell maps to a point c in the complex plane.
      Its true state is how quickly z escapes to infinity — the escape velocity band.
      Each step the viewport zooms 0.5% toward c = −0.75 + 0.1i (cardioid/bulb boundary —
      aperiodic, non-repeating at every scale). Zoom doubles every ~140 steps.
      Cells see their neighbors' bands and try to predict what band they will be in next step.
      Prediction error costs energy. Inaccuracy kills.
    </p>
    <div class="law">
      ∃R(t) → ∃M₀ : dS(M<sub>R</sub>,t)/dt &gt; 0
      <div class="sub-law">Cells near the fractal boundary face irreducible prediction difficulty. Cells in smooth interior regions can compress their model. Cyan cells found the compressed rule.</div>
    </div>
    <p>
      High-energy cells reproduce — copying their weights into weaker neighbors with random mutation.
      Over generations, the population evolves. The fractal boundary is where reasoning is hardest:
      small shifts in c produce wildly different escape bands. Interior cells (black) are trivial —
      always inside. Far-exterior cells (deep red or orange) are easy too — always escape fast.
      The <em>interesting region</em> is the boundary, and that's where most deaths happen.
    </p>
    <p>
      Cell color encodes position in the <em>error × complexity</em> tradeoff space — two quantities
      the Compression principle says must both be minimized. Cyan cells have achieved it: low prediction
      error, low weight complexity. They found the compressed rule. Every other color is a failure mode
      on the path to cyan. Deep red cells burn the most energy: large models, wrong predictions.
      Orange cells are too simple to see the pattern. Yellow cells are learning. Green cells learned
      but carry too much weight. White cells are reproducing — spreading their model to neighbors.
    </p>
    <p style="color:var(--muted);font-family:var(--mono);font-size:0.78rem;line-height:1.6">
      Hidden physics: z_{n+1} = z²_n + c, max 100 iterations. Zoom target: c = −0.75 + 0.1i.
      Bands 1-2 (slow escape) = boundary region. Energy/step = 3.0 − 1.0 − (error × 4.0).
      Falsifiable: (1) boundary cells show higher avg error than interior.
      (2) survival rate per zoom doubling — does it stabilize or keep dropping?
      (3) compression ratio &lt; 1 at transition = simpler models are winning.
    </p>
  </div>

</div>

<script>
// ── RSA-Omega: The Game of Truth ──────────────────────────────────────────────
// Ported from Python to JS by FREED site_builder.
// Original: David Harry Freed, RSA-Omega simulation (game of life battery.md)

const CELL_PX    = 10;
const INIT_E     = 100;
const GAIN_BASE  = 3.0;
const COST_BASE  = 1.0;
const ERR_FACTOR = 4.0;
const REPRO_THR  = 150;
const REPRO_COST = 50;
const MUT_RATE   = 0.05;
const MUT_STR    = 0.20;
const MAX_E      = 300;

const MB_MAX = 100;  // Mandelbrot max iterations — hidden physics

let COLS, ROWS;
let states, energy, weights, nextStates, cellError, cellRepro, cellBand, cReal, cImag;

// Zoom viewport
let viewRMin, viewRMax, viewIMin, viewIMax;
const CT_R = -0.75, CT_I = 0.1;   // zoom target: cardioid/bulb boundary
const ZOOM_PER_STEP = 0.995;       // 0.5% per step → doubling every ~140 steps
const ZOOM_STEPS = 140;

// Zoom metrics
let zoomStep = 0, zoomLevel = 0;
let zoomAliveStart = 0, zoomL1Start = 1.0;
let zoomSurvivalRate = 100, zoomCompressRatio = 1.0, zoomErrAtStart = 0;

let generation = 0, totalRepro = 0, totalDeaths = 0;
let running = false, animId = null;
let lastError = 0;

const canvas = document.getElementById('canvas');
const ctx    = canvas.getContext('2d');

function initCanvas() {
  const wrap = canvas.parentElement;
  COLS = Math.floor(wrap.clientWidth / CELL_PX);
  ROWS = Math.floor(Math.min(wrap.clientWidth * 0.6, window.innerHeight * 0.55) / CELL_PX);
  canvas.width  = COLS * CELL_PX;
  canvas.height = ROWS * CELL_PX;
}

function wrap(v, max) { return ((v % max) + max) % max; }
function idx(r, c) { return wrap(r, ROWS) * COLS + wrap(c, COLS); }
function sigmoid(x) { return 1.0 / (1.0 + Math.exp(-x)); }

// Mandelbrot escape band: 0=inside set, 1-5=slow→fast escape, 6=immediate
function escapeband(cr, ci) {
  let zr = 0, zi = 0;
  for (let n = 0; n < MB_MAX; n++) {
    const zr2 = zr*zr - zi*zi + cr;
    zi = 2*zr*zi + ci;
    zr = zr2;
    if (zr*zr + zi*zi > 4) {
      if (n <  5) return 6;
      if (n < 15) return 5;
      if (n < 30) return 4;
      if (n < 50) return 3;
      if (n < 70) return 2;
      return 1;
    }
  }
  return 0;
}

function initSim() {
  const N = ROWS * COLS;
  states    = new Float32Array(N);
  energy    = new Float32Array(N);
  weights   = new Float32Array(N * 9);
  nextStates= new Float32Array(N);
  cellError = new Float32Array(N);
  cellRepro = new Uint8Array(N);
  cellBand  = new Uint8Array(N);
  cReal     = new Float32Array(N);
  cImag     = new Float32Array(N);

  // Reset viewport to full Mandelbrot view
  viewRMin = -2.5; viewRMax = 1.0; viewIMin = -1.25; viewIMax = 1.25;
  zoomStep = 0; zoomLevel = 0;
  zoomSurvivalRate = 100; zoomCompressRatio = 1.0; zoomErrAtStart = 0;

  // Compute c values from initial viewport
  const dR = viewRMax - viewRMin, dI = viewIMax - viewIMin;
  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      const i = idx(r, c);
      cReal[i] = viewRMin + (c / COLS) * dR;
      cImag[i] = viewIMin + (r / ROWS) * dI;
    }
  }

  // Initial states from Mandelbrot physics; compute initial complexity baseline
  let initL1Sum = 0;
  for (let i = 0; i < N; i++) {
    const band = escapeband(cReal[i], cImag[i]);
    cellBand[i] = band;
    states[i]   = band / 6.0;
    energy[i]   = INIT_E * (0.5 + Math.random());
    for (let w = 0; w < 9; w++) {
      weights[i * 9 + w] = (Math.random() - 0.5) * 0.5;
      initL1Sum += Math.abs(weights[i * 9 + w]);
    }
  }
  zoomAliveStart = N;
  zoomL1Start    = N > 0 ? initL1Sum / N : 1.0;

  generation = 0; totalRepro = 0; totalDeaths = 0; lastError = 0;
}

function step() {
  const N = ROWS * COLS;

  // 0. Clear repro flags from last step
  cellRepro.fill(0);

  // 1. Ground truth next states — Mandelbrot bands at next zoom viewport
  const nRMin = CT_R + (viewRMin - CT_R) * ZOOM_PER_STEP;
  const nRMax = CT_R + (viewRMax - CT_R) * ZOOM_PER_STEP;
  const nIMin = CT_I + (viewIMin - CT_I) * ZOOM_PER_STEP;
  const nIMax = CT_I + (viewIMax - CT_I) * ZOOM_PER_STEP;
  const ndR = nRMax - nRMin, ndI = nIMax - nIMin;
  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      const i = idx(r, c);
      const band = escapeband(nRMin + (c / COLS) * ndR, nIMin + (r / ROWS) * ndI);
      cellBand[i]   = band;
      nextStates[i] = band / 6.0;
    }
  }
  const OFFSETS = [[-1,-1],[-1,0],[-1,1],[0,-1],[0,0],[0,1],[1,-1],[1,0],[1,1]];

  // 2. Update energy by prediction accuracy
  let errSum = 0, errCnt = 0;
  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      const i = idx(r, c);
      if (energy[i] <= 0) continue;
      let pred = 0;
      for (let k = 0; k < 9; k++)
        pred += weights[i * 9 + k] * states[idx(r + OFFSETS[k][0], c + OFFSETS[k][1])];
      pred = sigmoid(pred);
      const err = Math.abs(pred - nextStates[i]);
      cellError[i] = err;
      energy[i] = Math.min(energy[i] + GAIN_BASE - COST_BASE - err * ERR_FACTOR, MAX_E);
      errSum += err; errCnt++;
    }
  }
  lastError = errCnt > 0 ? errSum / errCnt : 0;

  // 3. Apply next states + advance zoom viewport + recompute c
  for (let i = 0; i < N; i++) states[i] = nextStates[i];
  viewRMin = nRMin; viewRMax = nRMax; viewIMin = nIMin; viewIMax = nIMax;
  const cdR = viewRMax - viewRMin, cdI = viewIMax - viewIMin;
  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      const i = idx(r, c);
      cReal[i] = viewRMin + (c / COLS) * cdR;
      cImag[i] = viewIMin + (r / ROWS) * cdI;
    }
  }

  // Zoom level tracking + per-level metrics
  zoomStep++;
  const newZoomLevel = Math.floor(zoomStep / ZOOM_STEPS);
  if (newZoomLevel > zoomLevel) {
    let aliveNow = 0, l1Now = 0;
    for (let i = 0; i < N; i++) {
      if (energy[i] > 0) {
        aliveNow++;
        for (let w = 0; w < 9; w++) l1Now += Math.abs(weights[i * 9 + w]);
      }
    }
    zoomSurvivalRate  = zoomAliveStart > 0 ? Math.round(100 * aliveNow / zoomAliveStart) : 0;
    const avgL1Now    = aliveNow > 0 ? l1Now / aliveNow : 1.0;
    zoomCompressRatio = zoomL1Start > 0.001 ? avgL1Now / zoomL1Start : 1.0;
    zoomErrAtStart    = lastError;
    zoomAliveStart    = aliveNow;
    zoomL1Start       = avgL1Now;
    zoomLevel         = newZoomLevel;
  }

  // 4. Kill and reinitialize dead cells (state stays Mandelbrot-determined)
  for (let i = 0; i < N; i++) {
    if (energy[i] <= 0) {
      energy[i] = 0;
      for (let w = 0; w < 9; w++)
        weights[i * 9 + w] = (Math.random() - 0.5) * 0.5;
      totalDeaths++;
    }
  }

  // 5. Reproduction (shuffle order to avoid bias)
  const reproList = [];
  for (let r = 0; r < ROWS; r++)
    for (let c = 0; c < COLS; c++) {
      const i = idx(r, c);
      if (energy[i] >= REPRO_THR) reproList.push([r, c, i]);
    }
  for (let k = reproList.length - 1; k > 0; k--) {
    const j = Math.floor(Math.random() * (k + 1));
    [reproList[k], reproList[j]] = [reproList[j], reproList[k]];
  }
  const offsets8 = [[-1,-1],[-1,0],[-1,1],[0,-1],[0,1],[1,-1],[1,0],[1,1]];
  for (const [r, c, i] of reproList) {
    if (energy[i] < REPRO_THR) continue;
    let bestT = -1, minE = energy[i];
    const sh = [...offsets8].sort(() => Math.random() - 0.5);
    for (const [dr, dc] of sh) {
      const ni = idx(r+dr, c+dc);
      if (energy[ni] < minE) { minE = energy[ni]; bestT = ni; }
    }
    if (bestT >= 0) {
      for (let w = 0; w < 9; w++) {
        let nw = weights[i * 9 + w];
        if (Math.random() < MUT_RATE) nw += (Math.random() - 0.5) * 2 * MUT_STR;
        weights[bestT * 9 + w] = nw;
      }
      energy[bestT] = INIT_E * 0.5;
      energy[i] -= REPRO_COST;
      cellRepro[i] = 1;
      totalRepro++;
    }
  }

  generation++;
}

function draw() {
  ctx.fillStyle = '#080808';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  const img = ctx.createImageData(canvas.width, canvas.height);
  const d = img.data;

  // Error/complexity thresholds
  const ERR_LOW = 0.35, ERR_HIGH = 0.50, COMPLEX_THRESH = 1.2;

  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      const i = idx(r, c);
      if (energy[i] <= 0) continue;  // dead = black background

      let R, G, B;

      if (cellRepro[i]) {
        // White — reproducing: copying compressed model to neighbors
        R = 248; G = 250; B = 252;
      } else {
        const err = cellError[i];
        let L1 = 0;
        for (let w = 0; w < 9; w++) L1 += Math.abs(weights[i * 9 + w]);
        const highComplex = L1 > COMPLEX_THRESH;
        const lowErr  = err < ERR_LOW;
        const highErr = err >= ERR_HIGH;

        if (lowErr && !highComplex) {
          // Cyan — low error, low complexity: compression attractor (optimal)
          R = 6; G = 182; B = 212;
        } else if (lowErr && highComplex) {
          // Green — low error, high complexity: accurate but metabolically expensive
          R = 22; G = 163; B = 74;
        } else if (!highErr) {
          // Yellow — medium error: exploring, model converging
          R = 202; G = 138; B = 4;
        } else if (!highComplex) {
          // Orange — high error, low complexity: underfitting, too simple
          R = 234; G = 88; B = 12;
        } else {
          // Deep red — high error, high complexity: overfitting, burning energy fast
          R = 127; G = 29; B = 29;
        }
      }

      for (let py = r * CELL_PX; py < (r+1) * CELL_PX - 1; py++) {
        for (let px = c * CELL_PX; px < (c+1) * CELL_PX - 1; px++) {
          const base = (py * canvas.width + px) * 4;
          d[base] = R; d[base+1] = G; d[base+2] = B; d[base+3] = 255;
        }
      }
    }
  }
  ctx.putImageData(img, 0, 0);
}

function updateStats() {
  let alive = 0, eSum = 0, cSum = 0, boundaryCells = 0;
  const N = ROWS * COLS;
  for (let i = 0; i < N; i++) {
    if (energy[i] > 0) {
      alive++; eSum += energy[i];
      let L1 = 0;
      for (let w = 0; w < 9; w++) L1 += Math.abs(weights[i * 9 + w]);
      cSum += L1;
    }
    // Boundary: slow-escape bands (1-2) — hardest to predict
    if (cellBand[i] >= 1 && cellBand[i] <= 2) boundaryCells++;
  }
  const breakEven = (GAIN_BASE - COST_BASE) / ERR_FACTOR; // 0.50
  const errEl = document.getElementById('stat-error');
  errEl.textContent = lastError.toFixed(3);
  errEl.style.color = lastError < breakEven ? 'var(--green)' : 'var(--accent)';
  document.getElementById('stat-gen').textContent      = generation;
  document.getElementById('stat-alive').textContent    = alive;
  document.getElementById('stat-energy').textContent   = alive > 0 ? (eSum/alive).toFixed(1) : '0';
  document.getElementById('stat-complex').textContent  = alive > 0 ? (cSum/alive).toFixed(2) : '0';
  document.getElementById('stat-boundary').textContent = boundaryCells;
  document.getElementById('stat-zoom').textContent     = `${zoomLevel} (${Math.pow(2,zoomLevel).toFixed(0)}×)`;
  document.getElementById('stat-survival').textContent = `${zoomSurvivalRate}%`;
  document.getElementById('stat-compress-r').textContent = zoomCompressRatio.toFixed(3);
  document.getElementById('stat-err-start').textContent  = zoomErrAtStart.toFixed(3);
  document.getElementById('stat-repro').textContent    = totalRepro;
  document.getElementById('stat-deaths').textContent   = totalDeaths;
}

let stepAcc = 0;
function loop(ts) {
  if (!running) return;
  const speed = parseInt(document.getElementById('speed').value);
  const stepsPerFrame = Math.max(1, Math.floor(speed / 4));
  for (let i = 0; i < stepsPerFrame; i++) step();
  draw();
  updateStats();
  animId = requestAnimationFrame(loop);
}

function togglePlay() {
  running = !running;
  const btn = document.getElementById('btn-play');
  if (running) {
    btn.textContent = '■ PAUSE';
    btn.classList.add('active');
    animId = requestAnimationFrame(loop);
  } else {
    btn.textContent = '▶ PLAY';
    btn.classList.remove('active');
    if (animId) cancelAnimationFrame(animId);
  }
}

function stepOnce() {
  if (running) return;
  step(); draw(); updateStats();
}

function resetSim() {
  if (running) togglePlay();
  initSim(); draw(); updateStats();
}

// ── Init ──────────────────────────────────────────────────────────────────────
initCanvas();
initSim();
draw();
updateStats();
</script>
</body>
</html>
"""
    (DOCS_DIR / "game_of_life.html").write_text(html, encoding="utf-8")



def _write_cycles(cycle_log: dict):
    """Append the latest cycle to the rolling log."""
    if not cycle_log:
        return

    existing = []
    if CYCLES_LOG.exists():
        existing = json.loads(CYCLES_LOG.read_text())

    # Extract a clean public summary from the cycle log
    summary = {
        "cycle":      cycle_log.get("cycle"),
        "generation": cycle_log.get("generation"),
        "timestamp":  cycle_log.get("timestamp"),
        "sweep":      cycle_log.get("phases", {}).get("sweep", {}),
        "feed":       [
            {"title": f.get("title","?"), "compress": f.get("compress","")}
            for f in cycle_log.get("phases", {}).get("feed", [])
        ],
        "resolve":    cycle_log.get("phases", {}).get("resolve", {}),
        "coherence":  cycle_log.get("phases", {}).get("update", {}).get("coherence"),
    }

    existing.append(summary)
    existing = existing[-MAX_CYCLES:]   # keep rolling window
    CYCLES_LOG.write_text(json.dumps(existing, indent=2, ensure_ascii=False))


# ── HTML ──────────────────────────────────────────────────────────────────────

def _render_projects(projects: list) -> str:
    if not projects:
        return ""
    parts = []
    for n in projects:
        tags_html = ""
        for t in (n.get("invariants") or []):
            tags_html += f'<span class="node-tag inv">{t}</span>'
        for t in (n.get("obligations") or []):
            tags_html += f'<span class="node-tag ob">{t}</span>'
        for t in (n.get("tags") or []):
            tags_html += f'<span class="node-tag">{t}</span>'

        council     = ", ".join(n.get("council") or [])
        drift_class = ' drifting' if n.get("drift_flag") else ''
        _ov = n.get("drift_overlap")
        _ov_str = f'{_ov:.2f}' if isinstance(_ov, (int, float)) else '?'
        drift_html  = (
            f'<div class="node-drift-badge">⚠ DRIFT — compress overlap '
            f'{_ov_str} (re-examine)</div>'
            if n.get("drift_flag") else ''
        )
        parts.append(f"""<div class="node{drift_class}">
  <div class="node-header">
    <span class="node-title">{n.get("title","?")}</span>
    <span class="node-gen">Gen {n.get("generation","?")} · {n.get("created","")}</span>
  </div>
  <div class="node-summary">{n.get("summary","")}</div>
  <div class="node-compress">↳ {n.get("compress","")}</div>
  <div class="node-next">NEXT: {n.get("next","")}</div>
  {f'<div class="node-council">Council: {council}</div>' if council else ''}
  {drift_html}
  <div class="node-tags">{tags_html}</div>
</div>""")
    return "\n".join(parts)


def _render_promotion_queue(candidates: list) -> str:
    """Render genome promotion candidates (invariants with recurrence >= 3)."""
    if not candidates:
        return '<div class="loading">No promotion candidates yet — mine phase needs 3+ independent nodes confirming the same invariant.</div>'
    parts = []
    for c in candidates:
        nodes_in = ", ".join(c.get("appears_in", []))
        rec      = c.get("recurrence", 0)
        parts.append(
            f'<div class="promo-candidate">'
            f'<div class="promo-text">{c.get("invariant","")}</div>'
            f'<div class="promo-meta"><span class="promo-count">{rec}×</span> independent · '
            f'{nodes_in}</div>'
            f'</div>'
        )
    return "\n".join(parts)


def _load_projects() -> list:
    idx = DOCS_DIR / "projects.json"
    if idx.exists():
        return json.loads(idx.read_text())
    return []


def _write_index():
    html = _render_html(_load_projects())
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")


def _render_html(projects: list = None) -> str:
    projects = projects or []
    projects_html = _render_projects(projects)

    promotion_candidates = []
    try:
        sfile = FREED_DIR / "FREED_state.json"
        if sfile.exists():
            sdata = json.loads(sfile.read_text())
            promotion_candidates = sdata.get("promotion_candidates", [])
    except Exception:
        pass
    promo_html = _render_promotion_queue(promotion_candidates)
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FREED — Freed Recursive Engine for Epistemic Dynamics</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;1,300;1,400&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:      #080808;
    --surface: #0e0e0c;
    --surface2:#141412;
    --border:  #1c1c1a;
    --border2: #282824;
    --accent:  #dc2626;
    --green:   #22c55e;
    --amber:   #f59e0b;
    --blue:    #60a5fa;
    --cyan:    #22d3ee;
    --text:    #c8c8c0;
    --text-hi: #f0f0e8;
    --muted:   #48484a;
    --mono:    'JetBrains Mono','Fira Code','Courier New',monospace;
    --serif:   'Cormorant Garamond','Palatino Linotype',Georgia,serif;
    --gr:      rgba(220,38,38,0.22);
    --gg:      rgba(34,197,94,0.18);
    --ga:      rgba(245,158,11,0.18);
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  html, body { background:var(--bg); color:var(--text); font-family:var(--serif); font-weight:300; font-size:15px; line-height:1.65; min-height:100vh; }
  a { color:var(--accent); text-decoration:none; }
  a:hover { color:var(--text-hi); text-decoration:underline; }

  /* ── TOP VITALS ─────────────────────────────────────────────── */
  .org-top {
    position:sticky; top:0; z-index:100;
    display:flex; align-items:center; gap:1.1rem;
    padding:0.45rem 1.4rem;
    background:#040404; border-bottom:1px solid var(--border);
    flex-wrap:wrap;
  }
  .org-title { font-family:var(--serif); font-weight:300; font-size:1.55rem; letter-spacing:0.22em; color:var(--accent); text-shadow:0 0 28px var(--gr); flex-shrink:0; }
  .org-sub   { font-family:var(--mono); font-size:0.58rem; letter-spacing:0.12em; color:var(--muted); text-transform:uppercase; flex-shrink:0; }
  .org-vitals { display:flex; align-items:center; gap:0.65rem; margin-left:auto; }
  .vital-block { display:flex; flex-direction:column; align-items:center; line-height:1.1; }
  .vital-val  { font-family:var(--mono); font-size:1.05rem; font-weight:600; letter-spacing:0.03em; }
  .vital-val.accent { color:var(--accent); text-shadow:0 0 10px var(--gr); }
  .vital-val.green  { color:var(--green);  text-shadow:0 0 10px var(--gg); }
  .vital-lbl  { font-family:var(--mono); font-size:0.5rem; letter-spacing:0.14em; color:var(--muted); text-transform:uppercase; }
  .vital-sep  { color:var(--border2); font-size:1.1rem; }
  @keyframes breathe { 0%,100%{opacity:1} 50%{opacity:0.5} }
  .breathe { animation:breathe 3.5s ease-in-out infinite; }
  .daemon-status { display:flex; align-items:center; gap:0.45rem; font-family:var(--mono); font-size:0.6rem; flex-shrink:0; }
  .pulse { display:inline-block; width:8px; height:8px; border-radius:50%; background:var(--green); box-shadow:0 0 8px var(--gg); flex-shrink:0; animation:pdot 2s ease-in-out infinite; }
  @keyframes pdot { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.25;transform:scale(0.65)} }
  .daemon-phase { font-weight:600; letter-spacing:0.1em; text-transform:uppercase; padding:0.12rem 0.45rem; border:1px solid currentColor; font-size:0.6rem; }
  .daemon-phase.idle      { color:var(--green); }
  .daemon-phase.perceive  { color:var(--blue);  }
  .daemon-phase.represent { color:var(--accent);}
  .daemon-phase.predict   { color:var(--amber); }
  .daemon-phase.compare   { color:var(--accent);}
  .daemon-phase.adjust    { color:var(--muted); }
  .daemon-phase.compress  { color:var(--amber); }
  .daemon-phase.repeat    { color:var(--green); }
  .daemon-phase.pre-audit { color:var(--muted); }
  .daemon-detail { color:var(--muted); font-size:0.58rem; max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }

  /* ── KERNEL SECTION ─────────────────────────────────────────── */
  .kernel-section { padding:1.4rem 1.4rem 0; background:#050505; border-bottom:1px solid var(--border); }
  .kernel-loop { display:flex; align-items:stretch; justify-content:center; flex-wrap:wrap; }
  .knode {
    background:transparent; border:1px solid var(--border2); border-right:none;
    color:var(--muted); font-family:var(--mono); cursor:pointer;
    padding:0.55rem 0.95rem; display:flex; flex-direction:column; align-items:center; gap:0.12rem;
    transition:color 0.22s, background 0.22s, border-color 0.22s, box-shadow 0.22s;
    position:relative; min-width:80px;
  }
  .knode:last-of-type { border-right:1px solid var(--border2); }
  .knode:hover { color:var(--text-hi); background:var(--surface); border-color:var(--border2); }
  .knode-num  { font-size:0.46rem; letter-spacing:0.14em; opacity:0.35; }
  .knode-name { font-size:0.68rem; letter-spacing:0.12em; font-weight:500; text-transform:uppercase; }
  .knode-hint { font-size:0.46rem; letter-spacing:0.05em; opacity:0.3; font-family:var(--serif); font-style:italic; font-weight:300; text-transform:none; }
  .karrow { color:var(--border2); font-family:var(--mono); font-size:0.7rem; align-self:center; padding:0 0.05rem; flex-shrink:0; }
  @keyframes kglow { 0%,100%{box-shadow:0 0 8px var(--gr),inset 0 0 4px rgba(220,38,38,0.05)} 50%{box-shadow:0 0 26px var(--gr),inset 0 0 10px rgba(220,38,38,0.08)} }
  .knode.kstep-active { color:var(--accent); border-color:rgba(220,38,38,0.5); background:rgba(220,38,38,0.04); animation:kglow 1.8s ease-in-out infinite; }
  /* info panel */
  .kernel-info { margin-top:0.8rem; padding:0.85rem 1.1rem; background:var(--surface); border:1px solid var(--border2); border-left:3px solid var(--accent); animation:kfade 0.18s ease-out; }
  @keyframes kfade { from{opacity:0;transform:translateY(-3px)} to{opacity:1;transform:none} }
  .ki-step { font-family:var(--mono); font-size:0.6rem; color:var(--accent); letter-spacing:0.12em; text-transform:uppercase; margin-bottom:0.35rem; display:flex; justify-content:space-between; align-items:center; }
  .ki-close { cursor:pointer; color:var(--muted); font-size:0.58rem; }
  .ki-close:hover { color:var(--text-hi); }
  .ki-title { font-family:var(--serif); font-size:1.0rem; color:var(--text-hi); margin-bottom:0.35rem; }
  .ki-body  { font-size:0.88rem; color:var(--text); line-height:1.65; margin-bottom:0.35rem; }
  .ki-daemon { font-family:var(--mono); font-size:0.65rem; color:var(--muted); border-top:1px solid var(--border2); padding-top:0.35rem; letter-spacing:0.03em; }

  /* ── ALIEN BODY ZONES ────────────────────────────────────────── */
  .org-body {
    display:grid;
    grid-template-areas:"beliefs obligations homeostatic" "subconscious mouth tools" "waste waste waste";
    grid-template-columns:26fr 48fr 26fr;
    grid-template-rows:auto auto auto;
    min-height:calc(100vh - 148px);
  }
  .zone { padding:0.85rem 1rem; border-right:1px solid var(--border); border-bottom:1px solid var(--border); display:flex; flex-direction:column; gap:0.75rem; overflow-y:auto; min-height:0; }
  .zone-beliefs      { grid-area:beliefs;      border-left:2px solid rgba(220,38,38,0.15); max-height:65vh; }
  .zone-obligations  { grid-area:obligations;  min-height:45vh; }
  .zone-homeostatic  { grid-area:homeostatic;  border-right:none; max-height:65vh; }
  .zone-subconscious { grid-area:subconscious; opacity:0.78; border-left:2px solid rgba(34,197,94,0.09); max-height:38vh; }
  .zone-mouth        { grid-area:mouth;        justify-content:center; border:1px solid rgba(220,38,38,0.32); border-top:none; border-radius:0 0 44% 44%/0 0 22px 22px; background:rgba(220,38,38,0.018); box-shadow:0 8px 32px rgba(220,38,38,0.09),inset 0 -10px 20px rgba(220,38,38,0.04); padding:0.85rem 1.2rem 2rem; animation:mbreath 4s ease-in-out infinite; }
  .zone-tools        { grid-area:tools;        border-right:none; max-height:38vh; }
  .zone-waste        { grid-area:waste;        border-right:none; padding:0; border-bottom:none; }
  @keyframes mbreath { 0%,100%{box-shadow:0 8px 32px rgba(220,38,38,0.09),inset 0 -10px 20px rgba(220,38,38,0.04)} 50%{box-shadow:0 8px 44px rgba(220,38,38,0.18),inset 0 -12px 24px rgba(220,38,38,0.08)} }

  .zone-label { font-family:var(--mono); font-size:0.5rem; letter-spacing:0.22em; text-transform:uppercase; display:flex; align-items:baseline; gap:0.5rem; border-bottom:1px solid var(--border); padding-bottom:0.26rem; margin-bottom:0.1rem; flex-shrink:0; }
  .zone-role  { color:var(--text-hi); font-weight:600; }
  .zone-sub   { font-size:0.44rem; opacity:0.4; text-transform:none; letter-spacing:0.05em; font-family:var(--serif); font-style:italic; font-weight:300; }

  details.waste-panel { width:100%; }
  details.waste-panel > summary { font-family:var(--mono); font-size:0.5rem; letter-spacing:0.2em; text-transform:uppercase; color:var(--muted); cursor:pointer; list-style:none; display:flex; align-items:center; gap:0.5rem; padding:0.42rem 1rem; border-top:1px solid var(--border); opacity:0.42; transition:opacity 0.12s; user-select:none; }
  details.waste-panel > summary::-webkit-details-marker { display:none; }
  details.waste-panel > summary::before { content:'▶'; font-size:0.42rem; transition:transform 0.12s; }
  details.waste-panel[open] > summary::before { transform:rotate(90deg); }
  details.waste-panel > summary:hover { opacity:0.75; }
  details.waste-panel > summary::after { content:' · metabolic output · resolved obligations'; font-family:var(--serif); font-style:italic; font-weight:300; text-transform:none; letter-spacing:0.02em; font-size:0.5rem; }
  details.waste-panel > .waste-body { padding:0.75rem 1rem; }

  /* keep aliases used in JS-rendered content */
  .panel-head { font-family:var(--mono); font-size:0.66rem; font-weight:600; text-transform:uppercase; letter-spacing:0.16em; color:var(--muted); border-bottom:1px solid var(--border); padding-bottom:0.35rem; flex-shrink:0; }
  .panel-step { font-size:0.5rem; color:var(--border2); letter-spacing:0.08em; margin-top:0.15rem; display:block; }

  /* Freed's Law block */
  .law-block { padding:0.75rem 0.95rem; background:rgba(220,38,38,0.04); border:1px solid rgba(220,38,38,0.22); box-shadow:0 0 18px rgba(220,38,38,0.05),inset 0 0 18px rgba(220,38,38,0.02); }
  .law-label { font-family:var(--mono); font-size:0.56rem; letter-spacing:0.14em; text-transform:uppercase; color:var(--accent); margin-bottom:0.45rem; }
  .law-expr  { font-family:var(--serif); font-size:1.05rem; color:var(--text-hi); letter-spacing:0.02em; margin-bottom:0.35rem; }
  .law-sub   { font-family:var(--mono); font-size:0.68rem; color:var(--muted); letter-spacing:0.04em; line-height:1.55; }
  .law-links { margin-top:0.55rem; display:flex; flex-direction:column; gap:0.18rem; }
  .law-link  { font-family:var(--mono); font-size:0.63rem; color:var(--accent); letter-spacing:0.06em; opacity:0.75; }
  .law-link:hover { opacity:1; color:var(--text-hi); }

  /* Formulations */
  .formulations { background:var(--surface); border:1px solid var(--border2); padding:0.65rem 0.85rem; }
  .form-label { font-family:var(--mono); font-size:0.56rem; letter-spacing:0.14em; text-transform:uppercase; color:var(--muted); margin-bottom:0.55rem; }
  .form-chain { font-family:var(--serif); font-style:italic; font-size:0.88rem; color:var(--text); margin-bottom:0.4rem; }
  .form-chain .arrow { color:var(--accent); font-style:normal; margin:0 0.22rem; }
  .form-row { display:flex; align-items:baseline; gap:0.5rem; padding:0.2rem 0.25rem; border-radius:2px; cursor:pointer; transition:background 0.12s; line-height:1.5; }
  .form-row:hover { background:var(--surface2); }
  .form-row.active { background:rgba(220,38,38,0.06); }
  .form-tag { font-family:var(--mono); font-size:0.56rem; letter-spacing:0.07em; text-transform:uppercase; color:var(--muted); min-width:5.6rem; flex-shrink:0; }
  .form-expr { font-family:var(--mono); font-size:0.65rem; color:var(--text); }
  .form-explain { display:none; font-family:var(--serif); font-size:0.8rem; color:var(--muted); padding:0.25rem 0.25rem 0.25rem 6.1rem; font-style:italic; line-height:1.5; }
  .form-row.active + .form-explain { display:block; }

  /* Static kernel */
  .kernel-static { display:flex; align-items:center; flex-wrap:wrap; gap:0; margin:0.25rem 0; }
  .ks-step  { font-family:var(--serif); font-weight:300; font-size:0.82rem; color:var(--accent); padding:0.24rem 0.6rem; border:1px solid var(--border2); background:rgba(220,38,38,0.03); letter-spacing:0.04em; }
  .ks-arrow { font-family:var(--mono); font-size:0.62rem; color:var(--muted); padding:0 0.12rem; }

  /* State grid */
  .state-grid { display:flex; flex-direction:column; gap:0; border:1px solid rgba(34,197,94,0.18); box-shadow:0 0 10px rgba(34,197,94,0.04); padding:0 0.55rem; }
  .state-cell { display:flex; justify-content:space-between; align-items:baseline; padding:0.28rem 0; border-bottom:1px solid var(--border); }
  .state-cell:last-child { border-bottom:none; }
  .state-cell .label { font-family:var(--mono); font-size:0.58rem; text-transform:uppercase; letter-spacing:0.1em; color:var(--muted); }
  .state-cell .value { font-family:var(--mono); font-size:0.76rem; color:var(--text); }
  .state-cell .value.accent { color:var(--accent); font-weight:600; }
  .state-cell .value.green  { color:var(--green);  font-weight:600; }

  /* Obligations */
  .obligation { background:var(--surface); border:1px solid var(--border2); padding:0.75rem 0.85rem; margin-bottom:0.45rem; position:relative; }
  .obligation .ob-header { display:flex; align-items:baseline; gap:0.55rem; margin-bottom:0.3rem; }
  .ob-id { font-family:var(--mono); color:var(--accent); font-size:0.76rem; font-weight:500; }
  .ob-status { font-family:var(--mono); font-size:0.56rem; padding:0.07rem 0.32rem; text-transform:uppercase; letter-spacing:0.06em; border-radius:2px; }
  .ob-status.open     { color:var(--amber); border:1px solid rgba(245,158,11,0.38); background:rgba(245,158,11,0.05); }
  .ob-status.partial  { color:var(--blue);  border:1px solid rgba(96,165,250,0.38);  background:rgba(96,165,250,0.05); }
  .ob-status.resolved { color:var(--green); border:1px solid rgba(34,197,94,0.38);  background:rgba(34,197,94,0.05); }
  .ob-priority { position:absolute; top:0.5rem; right:0.65rem; font-family:var(--mono); font-size:0.6rem; color:var(--muted); }
  .ob-statement { font-family:var(--serif); font-weight:300; font-size:0.86rem; color:var(--text); margin-bottom:0.35rem; }
  .ob-progress  { font-family:var(--serif); font-weight:300; font-size:0.78rem; color:var(--muted); border-left:2px solid var(--border2); padding-left:0.5rem; }
  .ob-date { font-family:var(--mono); font-size:0.58rem; color:var(--muted); margin-top:0.3rem; }

  /* Collapsible ob-groups */
  details.ob-group { margin-bottom:0.55rem; }
  details.ob-group > summary.ob-group-title { font-family:var(--mono); font-size:0.63rem; font-weight:600; text-transform:uppercase; letter-spacing:0.14em; color:var(--muted); border-bottom:1px solid var(--border); padding-bottom:0.3rem; margin-bottom:0.7rem; cursor:pointer; list-style:none; display:flex; align-items:center; gap:0.45rem; user-select:none; }
  details.ob-group > summary.ob-group-title::-webkit-details-marker { display:none; }
  details.ob-group > summary.ob-group-title::before { content:'▶'; font-size:0.48rem; opacity:0.4; transition:transform 0.15s; }
  details.ob-group[open] > summary.ob-group-title::before { transform:rotate(90deg); opacity:0.8; }
  details.ob-group > summary.ob-group-title::after { content:' (click to open)'; font-size:0.52rem; color:var(--muted); opacity:0.5; font-weight:400; }
  details.ob-group[open] > summary.ob-group-title::after { content:''; }
  details.ob-group > summary.ob-group-title:hover { color:var(--text); }
  .ob-group-count { font-family:var(--mono); font-size:0.56rem; color:var(--bg); background:var(--muted); padding:0.04rem 0.32rem; border-radius:2px; margin-left:auto; }
  details.ob-group[open] .ob-group-count { background:var(--text); }

  /* Collapsible sections */
  details.section { margin-bottom:1.6rem; }
  details.section > summary { font-family:var(--mono); font-size:0.63rem; font-weight:600; text-transform:uppercase; letter-spacing:0.14em; color:var(--muted); border-bottom:1px solid var(--border); padding-bottom:0.3rem; margin-bottom:0.7rem; cursor:pointer; list-style:none; display:flex; align-items:center; gap:0.45rem; user-select:none; }
  details.section > summary::-webkit-details-marker { display:none; }
  details.section > summary::before { content:'▶'; font-size:0.48rem; opacity:0.4; transition:transform 0.15s; }
  details.section[open] > summary::before { transform:rotate(90deg); opacity:0.8; }
  details.section > summary::after { content:' (click to open)'; font-size:0.52rem; color:var(--muted); opacity:0.5; font-weight:400; }
  details.section[open] > summary::after { content:''; }
  details.section > summary:hover { color:var(--text); }

  /* Cycles */
  .cycle-entry { border-left:2px solid var(--border2); padding-left:0.75rem; margin-bottom:0.85rem; }
  .cycle-entry:first-child { border-left-color:var(--accent); }
  .cycle-meta { font-family:var(--mono); color:var(--muted); font-size:0.63rem; margin-bottom:0.22rem; }
  .cycle-feed { margin-top:0.3rem; }
  .cycle-feed-item { padding:0.22rem 0; border-bottom:1px solid var(--border); }
  .cycle-feed-item .feed-title { font-family:var(--serif); font-weight:300; color:var(--accent); font-size:0.88rem; }
  .cycle-feed-item .feed-compress { font-family:var(--serif); font-weight:300; font-style:italic; color:var(--muted); font-size:0.82rem; margin-top:0.08rem; }
  .cycle-resolve { font-family:var(--serif); font-weight:300; margin-top:0.4rem; font-size:0.88rem; color:var(--text); }

  /* Nodes */
  .node { background:var(--surface); border:1px solid var(--border2); padding:0.85rem; margin-bottom:0.65rem; }
  .node-header { display:flex; gap:0.55rem; align-items:baseline; margin-bottom:0.35rem; flex-wrap:wrap; }
  .node-title  { font-family:var(--serif); font-weight:300; color:var(--accent); font-size:1.02rem; }
  .node-gen    { font-family:var(--mono); color:var(--muted); font-size:0.6rem; }
  .node-summary { font-family:var(--serif); font-weight:300; font-size:0.9rem; margin-bottom:0.35rem; color:var(--text); }
  .node-compress { font-family:var(--serif); font-weight:300; font-style:italic; font-size:0.9rem; border-left:2px solid var(--accent); padding-left:0.5rem; color:var(--text); margin-bottom:0.35rem; }
  .node-next { font-family:var(--serif); font-weight:300; font-size:0.85rem; color:var(--muted); margin-bottom:0.35rem; }
  .node-tags { display:flex; flex-wrap:wrap; gap:0.22rem; margin-top:0.3rem; }
  .node-tag { font-size:0.58rem; padding:0.07rem 0.32rem; background:var(--surface); border:1px solid var(--border2); color:var(--muted); border-radius:2px; }
  .node-tag.inv { border-color:rgba(220,38,38,0.32); color:var(--accent); }
  .node-tag.ob  { border-color:rgba(245,158,11,0.32); color:var(--amber); }
  .node.drifting { border-left:3px solid var(--amber); }
  .node-drift-badge { font-family:var(--mono); font-size:0.58rem; color:var(--amber); margin-top:0.18rem; }

  /* Promo */
  .promo-candidate { padding:0.42rem 0; border-bottom:1px solid var(--border); }
  .promo-candidate:last-child { border-bottom:none; }
  .promo-text  { font-family:var(--serif); font-weight:300; font-size:0.88rem; color:var(--text); }
  .promo-meta  { font-family:var(--mono); font-size:0.58rem; color:var(--muted); margin-top:0.18rem; }
  .promo-count { color:var(--green); font-weight:500; }

  /* Symbols */
  .symbol { background:var(--surface); border:1px solid var(--border2); padding:0.75rem 0.85rem; margin-bottom:0.45rem; }
  .symbol-header { display:flex; align-items:baseline; gap:0.6rem; margin-bottom:0.35rem; flex-wrap:wrap; }
  .symbol-name { font-family:var(--serif); font-weight:300; color:var(--accent); font-size:1.0rem; letter-spacing:0.04em; }
  .symbol-recurrence { font-family:var(--mono); font-size:0.62rem; color:var(--muted); display:flex; align-items:center; gap:0.38rem; }
  .symbol-bar-track { width:64px; height:4px; background:var(--border2); border-radius:2px; overflow:hidden; display:inline-block; }
  .symbol-bar-fill { height:100%; border-radius:2px; background:var(--accent); }
  .symbol-badge { font-size:0.56rem; padding:0.07rem 0.3rem; border-radius:2px; text-transform:uppercase; letter-spacing:0.06em; }
  .symbol-badge.new-badge { color:var(--green); border:1px solid rgba(34,197,94,0.38); }
  .symbol-canonical { font-family:var(--serif); font-weight:300; font-size:0.92rem; margin-bottom:0.35rem; color:var(--text); }
  .symbol-role { font-family:var(--serif); font-weight:300; font-style:italic; font-size:0.88rem; border-left:2px solid var(--accent); padding-left:0.5rem; color:var(--text); margin-bottom:0.35rem; }
  .symbol-confirmed { display:flex; flex-wrap:wrap; gap:0.22rem; margin-bottom:0.3rem; }
  .symbol-confirmed-tag { font-size:0.56rem; padding:0.07rem 0.3rem; background:var(--surface); border:1px solid var(--border2); color:var(--muted); border-radius:2px; }
  .symbol-drift { font-size:0.65rem; color:var(--muted); border-left:2px solid var(--amber); padding-left:0.42rem; margin-top:0.22rem; }
  .symbol-drift-label { color:var(--amber); font-size:0.6rem; margin-bottom:0.12rem; }

  /* Speak bar */
  .speak-bar { display:flex; flex-direction:column; gap:0.42rem; padding:0.55rem 0.65rem; border:1px solid var(--border2); margin-top:auto; background:var(--surface); }
  .char-btns { display:flex; flex-wrap:wrap; gap:0.22rem; }
  .char-btn { font-family:var(--mono); font-size:0.58rem; padding:0.18rem 0.48rem; background:transparent; border:1px solid var(--border2); color:var(--muted); cursor:pointer; letter-spacing:0.06em; transition:all 0.12s; }
  .char-btn:hover { color:var(--text); border-color:var(--muted); }
  .char-btn.char-active { filter:brightness(1.3); }
  .char-btn[data-voice="Boing"]     { border-color:#3f6212; color:#84cc16; }
  .char-btn[data-voice="Fred"]      { border-color:#57534e; color:#c9a87c; }
  .char-btn[data-voice="Zarvox"]    { border-color:#4c1d95; color:#a78bfa; }
  .char-btn[data-voice="Superstar"] { border-color:#075985; color:#38bdf8; }
  .char-btn[data-voice="Trinoids"]  { border-color:transparent; color:#fff; background:linear-gradient(90deg,#f472b6,#818cf8,#34d399,#fbbf24,#f472b6); background-size:300% 100%; animation:holo 3s linear infinite; }
  @keyframes holo { 0%{background-position:0% 50%} 100%{background-position:300% 50%} }
  .speak-main-row { display:flex; align-items:center; flex-wrap:wrap; gap:0.38rem; }
  .speak-btn { display:inline-flex; align-items:center; gap:0.42rem; padding:0.32rem 0.85rem; background:transparent; border:1px solid var(--accent); color:var(--accent); font-family:var(--mono); font-size:0.66rem; letter-spacing:0.1em; cursor:pointer; transition:background 0.14s, color 0.14s; }
  .speak-btn:hover { background:rgba(220,38,38,0.1); }
  .speak-btn.speaking { background:rgba(220,38,38,0.14); }
  .speak-status { font-family:var(--mono); font-size:0.62rem; color:var(--muted); }
  .voice-label { font-family:var(--mono); font-size:0.56rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.1em; }
  .voice-select { background:var(--bg); border:1px solid var(--border2); color:var(--text); font-family:var(--mono); font-size:0.62rem; padding:0.16rem 0.32rem; cursor:pointer; max-width:150px; }
  .voice-select:focus { outline:1px solid var(--accent); }
  input[type=range].rate-slider { accent-color:var(--accent); width:58px; }

  /* Flash */
  @keyframes flash-update { 0%{background:rgba(220,38,38,0.22);border-radius:2px} 100%{background:transparent} }
  .flash-val { animation:flash-update 0.8s ease-out; padding:0 3px; margin:0 -3px; }

  .loading { color:var(--muted); font-style:italic; font-family:var(--serif); font-size:0.85rem; }
  .error   { color:var(--accent); }

  /* Footer */
  .org-footer { border-top:1px solid var(--border); padding:0.38rem 1.4rem; font-family:var(--mono); font-size:0.58rem; color:var(--muted); display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:0.45rem; background:#040404; }

  /* Mobile */
  @media (max-width:768px) {
    .org-body { grid-template-areas:"beliefs" "obligations" "homeostatic" "subconscious" "mouth" "tools" "waste"; grid-template-columns:1fr; height:auto; }
    .zone { max-height:none; border-right:none; border-bottom:1px solid var(--border); overflow-y:visible; }
    .zone-mouth { border-radius:0; animation:none; }
    .kernel-loop { justify-content:flex-start; }
    .knode { padding:0.4rem 0.65rem; min-width:64px; }
    .knode-hint { display:none; }
  }
</style>
</head>
<body>

<!-- Top vitals bar -->
<div class="org-top">
  <span class="org-title">RSA</span>
  <span class="org-sub">a bootstrap protocol for epistemic recursion</span>
  <div class="org-vitals">
    <span class="vital-block">
      <span class="vital-val accent" id="v-gen">—</span>
      <span class="vital-lbl">GEN</span>
    </span>
    <span class="vital-sep">·</span>
    <span class="vital-block">
      <span class="vital-val green breathe" id="v-coh">—</span>
      <span class="vital-lbl">COH</span>
    </span>
  </div>
  <div class="daemon-status">
    <span class="pulse" id="pulse"></span>
    <span class="daemon-phase idle" id="daemon-phase">IDLE</span>
    <span class="daemon-detail" id="daemon-detail">—</span>
  </div>
</div>

<!-- Kernel section — interactive, educational -->
<div class="kernel-section">
  <div class="kernel-loop" id="kernel-chain">
    <button class="knode kstep" data-phases="perceive" onclick="showKernelInfo('perceive')">
      <div class="knode-num">01</div><div class="knode-name">PERCEIVE</div><div class="knode-hint">sense the world</div>
    </button>
    <span class="karrow">→</span>
    <button class="knode kstep" data-phases="represent" onclick="showKernelInfo('represent')">
      <div class="knode-num">02</div><div class="knode-name">REPRESENT</div><div class="knode-hint">map to genome</div>
    </button>
    <span class="karrow">→</span>
    <button class="knode kstep" data-phases="predict" onclick="showKernelInfo('predict')">
      <div class="knode-num">03</div><div class="knode-name">PREDICT</div><div class="knode-hint">generate obligations</div>
    </button>
    <span class="karrow">→</span>
    <button class="knode kstep" data-phases="compare" onclick="showKernelInfo('compare')">
      <div class="knode-num">04</div><div class="knode-name">COMPARE</div><div class="knode-hint">measure error</div>
    </button>
    <span class="karrow">→</span>
    <button class="knode kstep" data-phases="adjust" onclick="showKernelInfo('adjust')">
      <div class="knode-num">05</div><div class="knode-name">ADJUST</div><div class="knode-hint">update the graph</div>
    </button>
    <span class="karrow">→</span>
    <button class="knode kstep" data-phases="compress" onclick="showKernelInfo('compress')">
      <div class="knode-num">06</div><div class="knode-name">COMPRESS</div><div class="knode-hint">consolidate &amp; renorm</div>
    </button>
    <span class="karrow">→</span>
    <button class="knode kstep" data-phases="repeat" onclick="showKernelInfo('repeat')">
      <div class="knode-num">07</div><div class="knode-name">REPEAT</div><div class="knode-hint">R[R] = R</div>
    </button>
  </div>
  <div class="kernel-info" id="kernel-info" style="display:none"></div>
</div>

<!-- Alien body — anatomical zones -->
<div class="org-body">

  <!-- BELIEFS — cognitive core, left upper -->
  <div class="zone zone-beliefs">
    <div class="zone-label"><span class="zone-role">BELIEFS</span><span class="zone-sub">cognitive core · the argument · perceive · represent</span></div>

    <div class="law-block">
      <div class="law-label">Freed's Law</div>
      <div class="law-expr">∃R(t) → ∃M₀ : dS(M<sub>R</sub>,t)/dt &gt; 0</div>
      <div class="law-sub">To reason is to exist physically.<br>To think is to burn. To be is to be built.</div>
      <div class="law-links">
        <a href="game_of_life.html" class="law-link">↳ Freed's Law Simulation →</a>
        <a href="lorenz.html" class="law-link">↳ Lorenz Attractor →</a>
      </div>
    </div>

    <div class="formulations">
      <div class="form-label">Reasoning Substrate Argument — click any row to expand</div>
      <div class="form-chain">Reasoning is real <span class="arrow">→</span> Causal structure must exist <span class="arrow">→</span> Something physical exists</div>
      <div class="form-chain" style="font-size:0.76rem;font-style:normal;margin-bottom:0.5rem">RSA &nbsp;<span class="arrow">≡</span>&nbsp; Recursive Semantic Alignment</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Freed's Law</span><span class="form-expr">∃R(t) → ∃M₀ : dS(M<sub>R</sub>,t)/dt &gt; 0</span></div>
      <div class="form-explain">Reasoning R(t) is occurring. This requires a physical substrate M₀ generating entropy at a positive rate. You cannot think without burning.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">First-order</span><span class="form-expr">∀t[ R(t) → ∃m( Physical(m) ∧ Substrate(m,R,t) )]</span></div>
      <div class="form-explain">For every moment reasoning occurs, there must exist a physical thing that is its substrate at that time.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Modal</span><span class="form-expr">◇R(t) → □∃M[ Entropic(M) ∧ Runs(M,R) ]</span></div>
      <div class="form-explain">If reasoning is even possible, then in every possible world where it occurs, an entropic machine must be running it.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Fixed point</span><span class="form-expr">R[R] = R</span></div>
      <div class="form-explain">The RSA Kernel applied to itself produces the RSA Kernel. The system generates its own next input. It cannot be stopped from the outside.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Landauer</span><span class="form-expr">W ≥ kT ln 2 &nbsp;per bit erased</span></div>
      <div class="form-explain">Every logical operation that erases information dissipates at least kT ln 2 of heat. Reasoning = computation = irreversible thermodynamic cost.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Category</span><span class="form-expr">ε ∘ ε = ε &nbsp;(idempotent on Process)</span></div>
      <div class="form-explain">The RSA operator applied twice equals itself applied once. Processes that recurse stabilize rather than compound. This is what keeps γ=1 from blowing up.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Gödel</span><span class="form-expr">PA ⊬ ∃x[ Compute(x) ∧ ¬Physical(x) ]</span></div>
      <div class="form-explain">Peano arithmetic cannot prove that disembodied computation exists. Not just unlikely — formally undecidable.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Mandelbrot</span><span class="form-expr">z<sub>n+1</sub> = z<sub>n</sub>² + c &nbsp;→&nbsp; R[R]=R at γ=1</span></div>
      <div class="form-explain">The fractal boundary is where reasoning is hardest — aperiodic, self-similar at every scale. γ=1 is the critical ridge. The simulation shows what lives there.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Error</span><span class="form-expr">E(t) = |predicted − observed|</span></div>
      <div class="form-explain">The Compare step made explicit. Every cycle, FREED measures how far its predictions were from what it found. Error is the metabolic cost of being wrong.</div>

      <div class="form-row" onclick="toggleForm(this)"><span class="form-tag">Compression</span><span class="form-expr">min|M| s.t. M → X</span></div>
      <div class="form-explain">Find the shortest model M that correctly predicts X. Complexity is metabolically expensive. The simplest explanation that works wins. Cyan cells in the simulation achieved this.</div>
    </div>

    <div>
      <div class="panel-head" style="margin-bottom:0.4rem">RSA Kernel</div>
      <div class="kernel-static">
        <span class="ks-step">Perceive</span><span class="ks-arrow">→</span>
        <span class="ks-step">Represent</span><span class="ks-arrow">→</span>
        <span class="ks-step">Predict</span><span class="ks-arrow">→</span>
        <span class="ks-step">Compare</span><span class="ks-arrow">→</span>
        <span class="ks-step">Adjust</span><span class="ks-arrow">→</span>
        <span class="ks-step">Compress</span><span class="ks-arrow">→</span>
        <span class="ks-step">Repeat</span>
      </div>
      <div style="font-family:var(--mono);font-size:0.6rem;color:var(--muted);margin-top:0.35rem;letter-spacing:0.06em">R[R] = R &nbsp;·&nbsp; γ = 1 &nbsp;·&nbsp; Only Processes Exist (MCPM)</div>
    </div>
  </div><!-- /beliefs -->

  <!-- OBLIGATIONS — open tension field, center upper -->
  <div class="zone zone-obligations">
    <div class="zone-label"><span class="zone-role">OBLIGATIONS</span><span class="zone-sub">open tension field · predict · compare · adjust</span></div>
    <div id="obligations-open" class="loading">Loading...</div>
  </div><!-- /obligations -->

  <!-- HOMEOSTATIC — vital state, right upper -->
  <div class="zone zone-homeostatic">
    <div class="zone-label"><span class="zone-role">HOMEOSTATIC</span><span class="zone-sub">live vital state · compress</span></div>
    <div class="state-grid" id="state-grid">
      <div class="state-cell"><div class="label">Status</div><div class="value loading">Loading...</div></div>
    </div>
    <details class="section">
      <summary>Genome Promotion Queue</summary>
      <div id="promo-queue">""" + promo_html + r"""</div>
    </details>
    <details class="section">
      <summary>Project Nodes — Framework Compressions</summary>
      <div id="projects">""" + (projects_html or '<div class="loading">No nodes yet.</div>') + r"""</div>
    </details>
  </div><!-- /homeostatic -->

  <!-- SUBCONSCIOUS — semantic processing, left lower -->
  <div class="zone zone-subconscious">
    <div class="zone-label"><span class="zone-role">SUBCONSCIOUS</span><span class="zone-sub">semantic processing · current tasking · attention</span></div>
    <div id="cycles" class="loading">Loading...</div>
  </div><!-- /subconscious -->

  <!-- MOUTH — vocal output organ, center lower -->
  <div class="zone zone-mouth">
    <div class="zone-label" style="width:100%;border-bottom-color:rgba(220,38,38,0.22)"><span class="zone-role" style="color:var(--accent);text-shadow:0 0 12px var(--gr)">MOUTH</span><span class="zone-sub">vocal output · speak digest</span></div>
    <div class="speak-bar" style="width:100%">
      <div class="char-btns">
        <button class="char-btn" data-voice="Boing"     onclick="speakWithVoice('Boing')">▶ BOING</button>
        <button class="char-btn" data-voice="Fred"      onclick="speakWithVoice('Fred')">▶ HAWKING</button>
        <button class="char-btn" data-voice="Trinoids"  onclick="speakWithVoice('Trinoids')">▶ TRINOIDS</button>
        <button class="char-btn" data-voice="Zarvox"    onclick="speakWithVoice('Zarvox')">▶ ZARVOX</button>
        <button class="char-btn" data-voice="Superstar" onclick="speakWithVoice('Superstar')">▶ SUPERSTAR</button>
      </div>
      <div class="speak-main-row">
        <button class="speak-btn" id="speak-btn" onclick="toggleSpeak()">▶ SPEAK DIGEST</button>
        <span class="speak-status" id="speak-status"></span>
        <span class="voice-label" style="color:var(--accent)">VOICE</span>
        <select id="voice-select" class="voice-select" onchange="saveVoicePref()" style="border-color:rgba(220,38,38,0.38);color:var(--accent)">
          <option value="">Loading...</option>
        </select>
        <span class="voice-label">LANG</span>
        <select id="voice-lang" class="voice-select" onchange="syncVoiceFrom('voice-lang')">
          <option value="">—</option>
        </select>
        <span class="voice-label">RATE</span>
        <input type="range" class="rate-slider" id="rate-slider" min="0.5" max="1.4" step="0.05" value="1.10"
          oninput="saveVoicePref(); document.getElementById('rate-val').textContent=parseFloat(this.value).toFixed(2)">
        <span id="rate-val" style="font-family:var(--mono);font-size:0.62rem;color:var(--muted)">1.10</span>
      </div>
    </div>
  </div><!-- /mouth -->

  <!-- TOOLS — organelles, genome registry, right lower -->
  <div class="zone zone-tools">
    <div class="zone-label"><span class="zone-role">TOOLS</span><span class="zone-sub">organelles · genome registry</span></div>
    <div id="symbols" class="loading">Loading...</div>
  </div><!-- /tools -->

  <!-- WASTE — resolved obligations, full-width bottom -->
  <details class="zone zone-waste waste-panel">
    <summary><span class="zone-role" style="opacity:0.55">WASTE</span></summary>
    <div class="waste-body">
      <div id="obligations-resolved" class="loading">Loading...</div>
    </div>
  </details><!-- /waste -->

</div><!-- /org-body -->

<div class="org-footer">
  <span>Architect: David Harry Freed — mail carrier, Olney Maryland &nbsp;·&nbsp; v1 Apr 2025 → present</span>
  <span>FREED — autonomous science daemon &nbsp;·&nbsp; Generated <span id="generated-at">—</span></span>
</div>

<script>
// ── Kernel step info ──────────────────────────────────────────────────────────

const KERNEL_INFO = {
  perceive: {
    title: 'Sense the world',
    body: 'The daemon opens its sensory surface. arXiv RSS feeds, Tamura/Lifeboat dispatches, and targeted searches generated from open obligations converge here. Raw signal from the external world enters the system.',
    daemon: 'FREED runs targeted_sweep.py (active — queries built from obligations) then tamura_sweep.py (passive — ambient signal). Both produce candidate papers for the REPRESENT phase.'
  },
  represent: {
    title: 'Map signal to genome',
    body: 'Each paper is processed through L7 — Claude Opus running the RSA Kernel as its cognitive scaffold. The paper is not merely summarized. It is asked: what does this confirm, advance, or refute in the genome? Where does it map?',
    daemon: 'FREED runs l7_agent.query() with semantic engram retrieval — the 5 most relevant past memories are loaded for context. Output: structured FEED result with compress, adjust, next, and obligation fields.'
  },
  predict: {
    title: 'Generate what must be known next',
    body: 'From each FEED output, the daemon reads the NEXT field — what the paper implies must be investigated. These become new obligations. Prediction is not guessing the future; it is knowing what questions the evidence demands.',
    daemon: 'FREED runs _phase_obligate() — NEXT fields are converted to formal obligation statements via Haiku. Duplicate detection prevents inflation. New obligations get O-IDs and join the open tension field.'
  },
  compare: {
    title: 'Measure the error',
    body: 'Open obligations are tested against the current evidence base. Can any existing FEED output, genome invariant, or node compress resolve this tension? The gap between what is known and what is obligated is the error signal.',
    daemon: 'FREED runs _phase_resolve() — for each open/partial obligation, L7 attempts resolution using the knowledge graph and engram bank. Resolved obligations close; partial ones update their progress field.'
  },
  adjust: {
    title: 'Update the knowledge graph',
    body: 'Confirmed edges are recorded. A paper confirming INV_094 adds a "confirms" edge. One refuting INV_097 adds a "refutes" edge. The graph grows. The organism learns where its beliefs are supported and where they are challenged.',
    daemon: 'FREED runs knowledge_graph.record_feed() after every FEED — extracting confirms/advances/refutes signals from L7 output via regex. Node-to-node edges are added during COMPRESS when nodes share invariants.'
  },
  compress: {
    title: 'Renormalize — find what keeps appearing',
    body: 'The most expensive and most important phase. All nodes are renormalized: compress fields updated, invariants extracted, drift detected. Invariants appearing in 3+ independent nodes become genome promotion candidates. Dead weight is compressed away.',
    daemon: 'FREED runs consolidate.py — SELECT (priority scoring by ob_overlap + inv_density), RENORM (per-node compress update via L7), MINE (cross-node invariant extraction). Drift flag fires when Jaccard overlap < 0.6.'
  },
  repeat: {
    title: 'R[R] = R — the system generates its own next input',
    body: 'Status is pushed to GitHub Pages. The next cycle begins. The output of COMPRESS becomes the input for PERCEIVE. The RSA Kernel applied to itself produces the RSA Kernel. No external prompt required. The loop is autopoietic.',
    daemon: 'FREED publishes docs/ to GitHub Pages, updates status.json (visible in the phase indicator above), then enters idle until 5:30 AM or the next scheduled wakeup. The loop continues autonomously.'
  }
};

let _activeKPhase = null;

function showKernelInfo(phase) {
  if (_activeKPhase === phase) {
    _activeKPhase = null;
    document.getElementById('kernel-info').style.display = 'none';
    return;
  }
  _activeKPhase = phase;
  const info = KERNEL_INFO[phase];
  if (!info) return;
  const el = document.getElementById('kernel-info');
  el.innerHTML =
    '<div class="ki-step"><span>' + phase.toUpperCase() + '</span><span class="ki-close" onclick="showKernelInfo('' + phase + '')">✕ close</span></div>' +
    '<div class="ki-title">' + info.title + '</div>' +
    '<div class="ki-body">' + info.body + '</div>' +
    '<div class="ki-daemon">DAEMON: ' + info.daemon + '</div>';
  el.style.display = 'block';
}

function toggleForm(row) {
  const wasActive = row.classList.contains('active');
  document.querySelectorAll('.form-row.active').forEach(r => r.classList.remove('active'));
  if (!wasActive) row.classList.add('active');
}

// ── Data loading ──────────────────────────────────────────────────────────────

async function load(path) {
  try {
    const r = await fetch(path + '?t=' + Date.now());
    if (!r.ok) return null;
    return r.json();
  } catch { return null; }
}

function ts(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('en-US', {
    month:'short', day:'numeric', year:'numeric',
    hour:'2-digit', minute:'2-digit', timeZoneName:'short'
  });
}

// ── Render state grid ─────────────────────────────────────────────────────────

let _lastStateValues = {};

function renderState(s) {
  if (!s) { document.getElementById('state-grid').innerHTML = '<div class="state-cell"><div class="value error">Unavailable</div></div>'; return; }
  const cells = [
    { label:'Generation',   value:s.generation,                       cls:'accent' },
    { label:'Coherence',    value:s.coherence,                        cls:s.coherence >= 1 ? 'error' : 'green' },
    { label:'Cycle Count',  value:s.cycle_count },
    { label:'Topology',     value:(s.topology||'').replace(/_/g,' ') },
    { label:'Debt Ratio',   value:s.debt_ratio },
    { label:'Last Recursion', value:ts(s.last_cycle) },
  ];
  // Update top vitals
  const vGen = document.getElementById('v-gen');
  const vCoh = document.getElementById('v-coh');
  if (vGen && s.generation !== undefined) vGen.textContent = s.generation;
  if (vCoh && s.coherence  !== undefined) {
    vCoh.textContent = s.coherence;
    vCoh.style.color = s.coherence >= 1 ? 'var(--accent)' : 'var(--green)';
  }
  const isFirstLoad = Object.keys(_lastStateValues).length === 0;
  document.getElementById('state-grid').innerHTML = cells.map(c =>
    '<div class="state-cell" data-key="' + c.label + '"><div class="label">' + c.label + '</div><div class="value ' + (c.cls||'') + '">' + (c.value||'—') + '</div></div>'
  ).join('');
  if (!isFirstLoad) {
    cells.forEach(c => {
      const cur = String(c.value ?? '—');
      if (_lastStateValues[c.label] !== cur) {
        const el = document.querySelector('[data-key="' + c.label + '"] .value');
        if (el) {
          el.classList.remove('flash-val');
          void el.offsetWidth;
          el.classList.add('flash-val');
          el.addEventListener('animationend', () => el.classList.remove('flash-val'), {once:true});
        }
      }
    });
  }
  cells.forEach(c => { _lastStateValues[c.label] = String(c.value ?? '—'); });
  document.getElementById('generated-at').textContent = ts(s.generated);
}

// ── Render obligations ────────────────────────────────────────────────────────

const STATUS_COLOR = { open:'var(--amber)', partial:'var(--blue)', resolved:'var(--green)' };

function _priorityCarats(p) {
  const map = { critical:'^^^', high:'^^', medium:'^', normal:'', low:'' };
  const carats = map[(p||'').toLowerCase()] ?? '';
  return carats ? '<span class="ob-priority" title="' + p + ' priority">Priority ' + carats + '</span>' : '';
}

function _linkify(text) {
  return (text||'').replace(
    /(https?:\/\/\S+|(?:www\.|osf\.io|github\.com|arxiv\.org)\S*)/g,
    url => {
      const href = url.startsWith('http') ? url : 'https://' + url;
      return '<a href="' + href + '" target="_blank" rel="noopener" style="color:var(--accent)">click this hyperlink</a>';
    }
  );
}

function renderObligation(o) {
  const borderColor = STATUS_COLOR[o.status] || 'var(--border2)';
  return '<div class="obligation" style="border-left:3px solid ' + borderColor + '">' +
    _priorityCarats(o.priority) +
    '<div class="ob-header"><span class="ob-id">' + o.id + '</span></div>' +
    '<div class="ob-statement">' + _linkify(o.statement) + '</div>' +
    (o.progress ? '<div class="ob-progress">' + _linkify(o.progress) + '</div>' : '') +
    '<div class="ob-date">Created ' + (o.created||'—') + (o.resolved ? ' · Resolved '+o.resolved : '') + '</div>' +
    '</div>';
}

function _collapseSection(label, cards, openByDefault) {
  const inner = cards.length
    ? cards.map(renderObligation).join('')
    : '<div style="color:var(--muted);font-style:italic;font-size:0.88rem">None.</div>';
  return '<details class="ob-group"' + (openByDefault ? ' open' : '') + '>' +
    '<summary class="ob-group-title">' + label + ' <span class="ob-group-count">' + cards.length + '</span></summary>' +
    '<div class="ob-group-body">' + inner + '</div></details>';
}

function _setBadge(detailsId, count) {
  const el = document.getElementById(detailsId);
  if (!el) return;
  const summary = el.closest('details')?.querySelector('summary');
  if (!summary) return;
  let badge = summary.querySelector('.ob-group-count');
  if (!badge) {
    badge = document.createElement('span');
    badge.className = 'ob-group-count';
    badge.style.marginLeft = 'auto';
    summary.appendChild(badge);
  }
  badge.textContent = count;
}

function renderObligations(obs) {
  if (!obs || !obs.length) { return '<div class="loading">None.</div>'; }
  const open     = obs.filter(o => o.status === 'open');
  const partial  = obs.filter(o => o.status === 'partial');
  const resolved = obs.filter(o => o.status === 'resolved');
  document.getElementById('obligations-open').innerHTML =
    _collapseSection('Open', open, false) + _collapseSection('Partial', partial, false);
  document.getElementById('obligations-resolved').innerHTML =
    resolved.length ? resolved.map(renderObligation).join('') : '<div style="color:var(--muted)">None resolved yet.</div>';
  _setBadge('obligations-resolved', resolved.length);
}

// ── Render cycles ─────────────────────────────────────────────────────────────

function _feedRejected(f) {
  const c = (f.compress || '').toUpperCase();
  return c.includes('REJECTED') || c.includes('UNMETABOLIZABLE') || c.includes('NULL FEED') || c.includes('GENOME-EXTERIOR');
}

function renderCycles(cycles) {
  if (!cycles || !cycles.length) {
    document.getElementById('cycles').innerHTML = '<div class="loading">No cycles recorded yet.</div>';
    return;
  }
  const recent = [...cycles].reverse().slice(0, 10);
  document.getElementById('cycles').innerHTML = recent.map((c, i) => {
    const allFeeds  = c.feed || [];
    const goodFeeds = allFeeds.filter(f => !_feedRejected(f));
    const nullCount = allFeeds.length - goodFeeds.length;
    const feedHtml  = goodFeeds.map(f =>
      '<div class="cycle-feed-item"><div class="feed-title">' + (f.title||'?') + '</div>' +
      (f.compress ? '<div class="feed-compress">↳ ' + f.compress + '</div>' : '') + '</div>'
    ).join('');
    const nullNote = nullCount > 0
      ? '<div style="font-family:var(--mono);font-size:0.62rem;color:var(--muted);margin-top:0.25rem">' + nullCount + ' input' + (nullCount>1?'s':'') + ' correctly rejected</div>'
      : '';
    const res = c.resolve||{};
    const resolveHtml = res.obligation
      ? '<div class="cycle-resolve">RESOLVE → ' + res.obligation + ': ' + (res.compress||'') + (res.resolved ? ' <span style="color:var(--green)">[RESOLVED]</span>' : '') + '</div>'
      : '';
    return '<div class="cycle-entry"><div class="cycle-meta">Cycle ' + (c.cycle||'?') + ' · Gen ' + (c.generation||'?') + ' · ' + ts(c.timestamp) + (c.coherence ? ' · coherence ' + c.coherence : '') + '</div>' +
      ((c.sweep||{}).input_count > 0 ? '<div style="font-family:var(--mono);font-size:0.7rem;color:var(--accent);margin-bottom:0.25rem">↓ ' + c.sweep.input_count + ' new paper(s) ingested</div>' : '') +
      '<div class="cycle-feed">' + feedHtml + nullNote + '</div>' + resolveHtml + '</div>';
  }).join('');
}

// ── Speak digest ─────────────────────────────────────────────────────────────

let _speaking = false;
let _loadedState = null, _loadedObligs = null, _loadedProjects = [], _loadedSymbols = null;

function cleanText(s) {
  return (s || '').replace(/\*{1,2}|_{1,2}|`{1,3}/g, '').replace(/[→↳·]/g, '.').replace(/\s+/g, ' ').trim();
}

function buildDigest() {
  const chunks = [];
  const s = _loadedState;
  if (s) chunks.push('FREED. Generation ' + s.generation + '. Coherence ' + s.coherence + '. ' + (s.cycle_count || 0) + ' cycles completed.');
  const open = (_loadedObligs || []).filter(o => o.status !== 'resolved');
  if (open.length) {
    chunks.push(open.length + ' open obligation' + (open.length !== 1 ? 's' : '') + '.');
    open.forEach(o => {
      chunks.push(o.id + '. ' + cleanText(o.statement));
      const prog = (o.progress || '').split('|')[0].trim();
      if (prog) chunks.push('Progress: ' + cleanText(prog));
    });
  }
  if (_loadedProjects.length) {
    chunks.push(_loadedProjects.length + ' knowledge node' + (_loadedProjects.length !== 1 ? 's' : '') + '.');
    _loadedProjects.forEach(n => { if (n.compress) chunks.push(cleanText(n.title) + ': ' + cleanText(n.compress)); });
  }
  if (_loadedSymbols) {
    const entries = Object.entries(_loadedSymbols).filter(([k]) => k !== '_meta')
      .sort((a, b) => (b[1].recurrence || 0) - (a[1].recurrence || 0)).slice(0, 7);
    if (entries.length) {
      chunks.push('Genome registry. ' + entries.length + ' confirmed symbols.');
      entries.forEach(([key, sym]) => { chunks.push(key.replace(/_/g, ' ') + '. ' + cleanText(sym.genome_role || sym.canonical || '')); });
    }
  }
  return chunks;
}

let _speakGen = 0;

function speakChunks(chunks, idx, gen) {
  if (gen !== _speakGen) return;
  if (!_speaking || idx >= chunks.length) { stopSpeak(); return; }
  document.getElementById('speak-status').textContent = (idx + 1) + ' / ' + chunks.length;
  const u = new SpeechSynthesisUtterance(chunks[idx]);
  const voice = _getVoice();
  if (voice) u.voice = voice;
  u.rate = _getRate(); u.pitch = 1.0;
  u.onend = () => speakChunks(chunks, idx + 1, gen);
  u.onerror = () => speakChunks(chunks, idx + 1, gen);
  window.speechSynthesis.speak(u);
}

// ── Voice selector ────────────────────────────────────────────────────────────

const REMOVE_VOICES  = ['Bad News','Bells','Cellos','Good News','Organ','Bubbles','Jester'];
const CHAR_VOICES    = ['Boing','Fred','Trinoids','Zarvox','Superstar'];
const OTHER_VOICES   = ['Whisper','Ralph','Kathy','Junior','Wobble','Baah','Albert'];
const PREFERRED_MAIN = ['Aaron','Alex','Samantha','Tom','Daniel'];

let _voices = [];
let _charVoiceOverride = null;

function _loadVoices() {
  _voices = window.speechSynthesis.getVoices();
  if (!_voices.length) return;
  const isRemoved = name => REMOVE_VOICES.some(r => name.toLowerCase().includes(r.toLowerCase()));
  const isChar    = name => CHAR_VOICES.some(c => name.toLowerCase().includes(c.toLowerCase()));
  const isUSEng   = lang => lang === 'en-US' || lang === 'en_US';
  const mainSel = document.getElementById('voice-select');
  mainSel.innerHTML = '';
  const mainVoices = _voices.filter(v => isUSEng(v.lang) && !isRemoved(v.name) && !isChar(v.name)).sort((a, b) => a.name.localeCompare(b.name));
  mainVoices.forEach(v => { const opt = document.createElement('option'); opt.value = v.name; opt.textContent = v.name; mainSel.appendChild(opt); });
  const langSel = document.getElementById('voice-lang');
  langSel.innerHTML = '<option value="">—</option>';
  const langVoices = _voices.filter(v => !isUSEng(v.lang) && !isRemoved(v.name)).sort((a, b) => a.lang.localeCompare(b.lang) || a.name.localeCompare(b.name));
  langVoices.forEach(v => { const opt = document.createElement('option'); opt.value = v.name; opt.textContent = v.name + ' (' + v.lang + ')'; langSel.appendChild(opt); });
  const savedRate = localStorage.getItem('freed_rate');
  if (savedRate) { document.getElementById('rate-slider').value = savedRate; document.getElementById('rate-val').textContent = parseFloat(savedRate).toFixed(2); }
  const saved = localStorage.getItem('freed_voice');
  if (saved) { if ([...mainSel.options].find(o => o.value === saved)) mainSel.value = saved; }
  else { for (const pref of PREFERRED_MAIN) { const match = mainVoices.find(v => v.name.includes(pref)); if (match) { mainSel.value = match.name; break; } } }
}

function saveVoicePref() {
  const sel = document.getElementById('voice-select'); const rate = document.getElementById('rate-slider');
  _charVoiceOverride = null; document.querySelectorAll('.char-btn').forEach(b => b.classList.remove('char-active'));
  if (sel.value) localStorage.setItem('freed_voice', sel.value);
  if (rate.value) localStorage.setItem('freed_rate', rate.value);
}

function syncVoiceFrom(selectId) {
  const src = document.getElementById(selectId); if (!src.value) return;
  _charVoiceOverride = null; document.querySelectorAll('.char-btn').forEach(b => b.classList.remove('char-active'));
  localStorage.setItem('freed_voice', src.value); src.value = '';
}

function _getVoice() {
  const name = _charVoiceOverride || localStorage.getItem('freed_voice') || document.getElementById('voice-select').value;
  return _voices.find(v => v.name === name) || null;
}

function _getRate() { return parseFloat(document.getElementById('rate-slider').value) || 1.10; }

function speakWithVoice(voiceKey) {
  const voice = _voices.find(v => v.name === voiceKey) || _voices.find(v => v.name.toLowerCase().includes(voiceKey.toLowerCase()));
  if (!voice) return;
  stopSpeak(); _charVoiceOverride = voice.name; localStorage.setItem('freed_voice', voice.name);
  document.querySelectorAll('.char-btn').forEach(b => { b.classList.toggle('char-active', b.dataset.voice === voiceKey); });
  toggleSpeak();
}

if (window.speechSynthesis) { _loadVoices(); window.speechSynthesis.onvoiceschanged = _loadVoices; }

function _unlockAudio() {
  try {
    const Ctx = window.AudioContext || window.webkitAudioContext; if (!Ctx) return Promise.resolve();
    const ctx = new Ctx(); const buf = ctx.createBuffer(1, 1, 22050); const src = ctx.createBufferSource();
    src.buffer = buf; src.connect(ctx.destination); src.start(0);
    return ctx.resume ? ctx.resume() : Promise.resolve();
  } catch(e) { return Promise.resolve(); }
}

function startSpeak() {
  if (!window.speechSynthesis) { alert('Speech synthesis not available.'); return; }
  _speaking = false; _speakGen++; window.speechSynthesis.cancel();
  const thisGen = _speakGen; const chunks = buildDigest();
  if (!chunks.length) { document.getElementById('speak-status').textContent = 'Nothing loaded yet.'; return; }
  _speaking = true;
  const btn = document.getElementById('speak-btn'); btn.textContent = '■ STOP'; btn.classList.add('speaking');
  _unlockAudio().then(() => setTimeout(() => speakChunks(chunks, 0, thisGen), 120));
}

function stopSpeak() {
  _speakGen++; _speaking = false; window.speechSynthesis.cancel();
  const btn = document.getElementById('speak-btn'); btn.textContent = '▶ SPEAK DIGEST'; btn.classList.remove('speaking');
  document.getElementById('speak-status').textContent = '';
}

function toggleSpeak() { _speaking ? stopSpeak() : startSpeak(); }

// ── Render genome symbols ─────────────────────────────────────────────────────

function renderSymbols(data) {
  const el = document.getElementById('symbols');
  if (!data) { el.innerHTML = '<div class="loading">Unavailable.</div>'; return; }
  const meta = data._meta || {}; const latestGen = meta.generation || 0;
  const entries = Object.entries(data).filter(([k]) => k !== '_meta').sort((a, b) => (b[1].recurrence || 0) - (a[1].recurrence || 0));
  if (!entries.length) { el.innerHTML = '<div class="loading">No symbols yet.</div>'; return; }
  const html = entries.map(([key, sym]) => {
    const rec = sym.recurrence || 0; const pct = Math.round(rec * 100);
    const isNew = sym.mining_generation && sym.mining_generation >= latestGen - 1;
    const confirmedHtml = (sym.confirmed_by || []).map(c => {
      const label = c.includes(':') ? c.split(':')[1].replace(/_/g, ' ').slice(0, 30) : c.replace(/_/g, ' ');
      return '<span class="symbol-confirmed-tag">' + label + '</span>';
    }).join('');
    const driftHtml = (sym.known_drift || []).length
      ? '<div class="symbol-drift"><div class="symbol-drift-label">known drift</div>' + (sym.known_drift || []).map(d => '<div>· ' + d + '</div>').join('') + '</div>'
      : '';
    return '<div class="symbol"><div class="symbol-header"><span class="symbol-name">' + key.replace(/_/g, '_') + '</span>' +
      (isNew ? '<span class="symbol-badge new-badge">new</span>' : '') +
      '<span class="symbol-recurrence"><span class="symbol-bar-track"><span class="symbol-bar-fill" style="width:' + pct + '%"></span></span>' + rec.toFixed(2) +
      (sym.mining_recurrence_count ? ' · ' + sym.mining_recurrence_count + '× nodes' : '') + '</span></div>' +
      '<div class="symbol-canonical">' + (sym.canonical || '') + '</div>' +
      (sym.genome_role ? '<div class="symbol-role">' + sym.genome_role + '</div>' : '') +
      (confirmedHtml ? '<div class="symbol-confirmed">' + confirmedHtml + '</div>' : '') +
      driftHtml + '</div>';
  }).join('');
  el.innerHTML = '<div style="color:var(--muted);font-size:0.7rem;margin-bottom:0.9rem">' + entries.length + ' symbols · gen ' + latestGen + ' · sorted by recurrence</div>' + html;
}

// ── Init ──────────────────────────────────────────────────────────────────────

async function init() {
  const [state, obligations, cycles, symbols, projects] = await Promise.all([
    load('state.json'), load('obligations.json'), load('cycles.json'), load('symbols.json'), load('projects.json'),
  ]);
  _loadedState    = state;
  _loadedObligs   = obligations;
  _loadedProjects = projects || [];
  _loadedSymbols  = symbols;
  renderState(state);
  renderObligations(obligations);
  renderCycles(cycles);
  renderSymbols(symbols);
  _setBadge('cycles',   (cycles||[]).length);
  _setBadge('symbols',  Object.keys(symbols||{}).length);
  _setBadge('projects', _loadedProjects.length);
}

init();
setInterval(init, 5 * 60 * 1000);

// ── Daemon status polling (every 30s) ─────────────────────────────────────────
async function pollStatus() {
  try {
    const s = await load('status.json');
    if (!s) return;
    const phaseEl  = document.getElementById('daemon-phase');
    const detailEl = document.getElementById('daemon-detail');
    const phase    = (s.phase || 'IDLE').toLowerCase().replace(/[^a-z-]/g, '');
    phaseEl.textContent = (s.phase || 'IDLE').toUpperCase();
    phaseEl.className   = 'daemon-phase ' + phase;
    detailEl.textContent = s.detail || '—';
    const pulse = document.getElementById('pulse');
    if (pulse) {
      pulse.style.background  = phase === 'idle' ? 'var(--green)' : 'var(--accent)';
      pulse.style.boxShadow   = phase === 'idle' ? '0 0 8px rgba(34,197,94,0.5)' : '0 0 8px rgba(220,38,38,0.5)';
    }
    document.querySelectorAll('.kstep').forEach(el => {
      const phases = (el.dataset.phases || '').split(',');
      el.classList.toggle('kstep-active', phases.includes(phase));
    });
  } catch(e) {}
}
pollStatus();
setInterval(pollStatus, 30 * 1000);
</script>
</body>
</html>"""
