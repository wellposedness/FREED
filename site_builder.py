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
    _write_lorenz()
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
    """Write the RSA-Omega Game of Truth simulation page."""
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
    <div class="sub">Each cell is an agent. The universe has a hidden physics. Survival requires modeling it.</div>
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
    <div class="stat-row"><span class="label">Reproductions</span><span class="value" id="stat-repro">0</span></div>
    <div class="stat-row"><span class="label">Deaths</span><span class="value" id="stat-deaths">0</span></div>
  </div>

  <div class="section">
    <div class="section-title">Color Legend</div>
    <div class="legend">
      <div class="legend-item"><div class="legend-swatch" style="background:#080808;border:1px solid #333"></div>Dead / zero energy</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#440808"></div>Struggling</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#882020"></div>Surviving</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#b91c1c"></div>Thriving</div>
      <div class="legend-item"><div class="legend-swatch" style="background:#fca5a5"></div>Reproducing</div>
    </div>
  </div>

  <div class="section theory">
    <div class="section-title">What This Is</div>
    <p>
      A cellular automaton where each cell carries a private 3×3 weight matrix —
      a local generative model of the universe. The universe obeys a fixed hidden physics kernel
      the cells do not know. At each step, every cell predicts its own next state using its weights.
      The prediction error is subtracted from its metabolic energy. Accurate cells gain net energy.
      Inaccurate cells die.
    </p>
    <div class="law">
      ∃R(t) → ∃M₀ : dS(M<sub>R</sub>,t)/dt &gt; 0
      <div class="sub-law">Cells whose survival is coupled to prediction error evolve toward models that approximate true dynamics. Reasoning = survival.</div>
    </div>
    <p>
      High-energy cells reproduce — copying their weights into weaker neighbors with random mutation.
      Over generations, the population evolves. Cells that build better internal models of the hidden
      physics survive. Cells that don't, die. This is Freed's Law as selection pressure:
      <em>to think accurately is to live</em>.
    </p>
    <p style="color:var(--muted);font-family:var(--mono);font-size:0.78rem;line-height:1.6">
      Hidden kernel: [0.1, 0.3, 0.1 / 0.3, −0.6, 0.3 / 0.1, 0.3, 0.1] — Mexican-hat convolution.
      Cells do not know this. They infer it through survival.
      Energy/step = 3.0 − 1.0 − (error × 4.0). Break-even: 0.50.
      Watch avg error — when it drops below 0.50, the population has learned.
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

// Hidden physics kernel (Mexican hat, 3x3 row-major)
const KERNEL = [0.1, 0.3, 0.1, 0.3, -0.6, 0.3, 0.1, 0.3, 0.1];

let COLS, ROWS;
let states, energy, weights, nextStates;
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

function initSim() {
  const N = ROWS * COLS;
  states    = new Float32Array(N);
  energy    = new Float32Array(N);
  weights   = new Float32Array(N * 9);
  nextStates= new Float32Array(N);
  for (let i = 0; i < N; i++) {
    states[i]  = Math.random() > 0.5 ? 1 : 0;
    energy[i]  = INIT_E * (0.5 + Math.random());
    for (let w = 0; w < 9; w++)
      weights[i * 9 + w] = (Math.random() - 0.5) * 0.5;
  }
  generation = 0; totalRepro = 0; totalDeaths = 0; lastError = 0;
}

function step() {
  const N = ROWS * COLS;

  // 1. Ground truth next states
  const OFFSETS = [[-1,-1],[-1,0],[-1,1],[0,-1],[0,0],[0,1],[1,-1],[1,0],[1,1]];
  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      let sum = 0;
      for (let k = 0; k < 9; k++) {
        sum += KERNEL[k] * states[idx(r + OFFSETS[k][0], c + OFFSETS[k][1])];
      }
      nextStates[idx(r, c)] = sigmoid(sum) > 0.5 ? 1 : 0;
    }
  }

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
      energy[i] = Math.min(energy[i] + GAIN_BASE - COST_BASE - err * ERR_FACTOR, MAX_E);
      errSum += err; errCnt++;
    }
  }
  lastError = errCnt > 0 ? errSum / errCnt : 0;

  // 3. Apply next states
  for (let i = 0; i < N; i++) states[i] = nextStates[i];

  // 4. Kill and reinitialize dead cells
  for (let i = 0; i < N; i++) {
    if (energy[i] <= 0) {
      energy[i] = 0;
      states[i] = Math.random() > 0.5 ? 1 : 0;
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

  for (let r = 0; r < ROWS; r++) {
    for (let c = 0; c < COLS; c++) {
      const e = energy[idx(r, c)];
      if (e <= 0) continue;
      const t = Math.min(e / MAX_E, 1.0);
      let R, G, B;
      if (t < 0.33) {
        // black → deep red
        const s = t / 0.33;
        R = Math.floor(s * 100); G = 0; B = 0;
      } else if (t < 0.66) {
        // deep red → accent red #cc2222
        const s = (t - 0.33) / 0.33;
        R = Math.floor(100 + s * 104);
        G = Math.floor(s * 34);
        B = Math.floor(s * 34);
      } else {
        // accent red → hot white
        const s = (t - 0.66) / 0.34;
        R = Math.floor(204 + s * 51);
        G = Math.floor(34  + s * 110);
        B = Math.floor(34  + s * 110);
      }
      for (let py = r * CELL_PX; py < (r+1) * CELL_PX - 1; py++) {
        for (let px = c * CELL_PX; px < (c+1) * CELL_PX - 1; px++) {
          const base = (py * canvas.width + px) * 4;
          d[base]   = R; d[base+1] = G; d[base+2] = B; d[base+3] = 255;
        }
      }
    }
  }
  ctx.putImageData(img, 0, 0);
}

function updateStats() {
  let alive = 0, eSum = 0;
  const N = ROWS * COLS;
  for (let i = 0; i < N; i++) {
    if (energy[i] > 0) { alive++; eSum += energy[i]; }
  }
  const breakEven = (GAIN_BASE - COST_BASE) / ERR_FACTOR; // 0.50
  const errEl = document.getElementById('stat-error');
  errEl.textContent = lastError.toFixed(3);
  errEl.style.color = lastError < breakEven ? 'var(--green)' : 'var(--accent)';
  document.getElementById('stat-gen').textContent    = generation;
  document.getElementById('stat-alive').textContent  = alive;
  document.getElementById('stat-energy').textContent = alive > 0 ? (eSum/alive).toFixed(1) : '0';
  document.getElementById('stat-repro').textContent  = totalRepro;
  document.getElementById('stat-deaths').textContent = totalDeaths;
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


def _write_lorenz():
    """Lorenz strange attractor — rotating 3D trail, FREED aesthetic."""
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lorenz Attractor — FREED</title>
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
    padding: 1.5rem;
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  .container { max-width: 960px; margin: 0 auto; }
  .header { border-bottom: 1px solid var(--border); padding-bottom: 0.9rem; margin-bottom: 1.4rem; }
  .header .nav { font-family: var(--mono); font-size: 0.72rem; color: var(--muted); margin-bottom: 0.5rem; letter-spacing: 0.06em; }
  .header h1 { font-family: var(--serif); font-weight: 300; font-size: 1.9rem; color: var(--accent); letter-spacing: 0.02em; }
  .header .sub { font-family: var(--serif); font-weight: 300; font-style: italic; color: var(--muted); font-size: 1rem; margin-top: 0.2rem; }
  .sim-wrap { width: 100%; background: #080808; border: 1px solid var(--border); margin-bottom: 0.9rem; line-height: 0; }
  canvas { display: block; width: 100%; height: auto; }
  .controls { display: flex; flex-wrap: wrap; gap: 0.45rem; align-items: center; margin-bottom: 0.9rem; }
  .btn {
    padding: 0.35rem 0.85rem; background: transparent;
    border: 1px solid var(--accent); color: var(--accent);
    font-family: var(--mono); font-size: 0.72rem; letter-spacing: 0.07em;
    cursor: pointer; transition: background 0.12s, color 0.12s;
  }
  .btn:hover, .btn.active { background: var(--accent); color: var(--bg); }
  .speed-wrap { display: flex; align-items: center; gap: 0.45rem; font-family: var(--mono); font-size: 0.68rem; color: var(--muted); }
  input[type=range] { accent-color: var(--accent); width: 80px; }
  .stats { margin-bottom: 1.4rem; }
  .stat-row { display: flex; justify-content: space-between; align-items: baseline; padding: 0.28rem 0; border-bottom: 1px solid var(--border); }
  .stat-row:last-child { border-bottom: none; }
  .stat-row .label { font-family: var(--mono); color: var(--muted); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.08em; }
  .stat-row .value { font-family: var(--mono); font-size: 0.82rem; color: var(--text); }
  .stat-row .value.accent { color: var(--accent); font-weight: 600; }
  .section { margin-bottom: 1.6rem; }
  .section-title { font-family: var(--mono); font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.12em; color: var(--text); border-bottom: 1px solid var(--border); padding-bottom: 0.35rem; margin-bottom: 0.8rem; }
  .theory p { font-family: var(--serif); font-weight: 300; font-size: 1rem; margin-bottom: 0.7rem; color: var(--text); }
  .theory em { font-style: italic; }
  .theory .law { font-family: var(--mono); font-size: 0.88rem; background: var(--surface); border-left: 3px solid var(--accent); padding: 0.7rem 1rem; margin: 0.9rem 0; line-height: 1.8; }
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <div class="nav"><a href="index.html">← FREED</a></div>
    <h1>Lorenz Attractor — Dissipative Chaos</h1>
    <div class="sub">Deterministic. Bounded. Never repeating. A portrait of structured entropy.</div>
  </div>

  <div class="controls">
    <button class="btn" id="btn-play" onclick="togglePlay()">⏸ PAUSE</button>
    <button class="btn" onclick="resetSim()">RESET</button>
    <div class="speed-wrap">SPEED <input type="range" id="speed" min="1" max="20" value="6"></div>
    <div class="speed-wrap">TRAIL <input type="range" id="trail" min="500" max="8000" value="3000" step="500"></div>
  </div>

  <div class="sim-wrap">
    <canvas id="canvas"></canvas>
  </div>

  <div class="stats">
    <div class="stat-row"><span class="label">t</span><span class="value accent" id="stat-t">0.000</span></div>
    <div class="stat-row"><span class="label">x · y · z</span><span class="value" id="stat-xyz">—</span></div>
    <div class="stat-row"><span class="label">σ (Prandtl)</span><span class="value" id="stat-sigma">10</span></div>
    <div class="stat-row"><span class="label">ρ (Rayleigh)</span><span class="value" id="stat-rho">28</span></div>
    <div class="stat-row"><span class="label">β</span><span class="value" id="stat-beta">2.667</span></div>
    <div class="stat-row"><span class="label">Lyapunov λ₁</span><span class="value" id="stat-lyap">≈ 0.906</span></div>
  </div>

  <div class="section theory">
    <div class="section-title">What This Is</div>
    <p>
      The Lorenz system is a three-dimensional dissipative dynamical system. It was derived
      by Edward Lorenz in 1963 as a simplified model of atmospheric convection — a fluid heated
      from below, cooled from above. Despite having only three variables and three parameters,
      it produces behavior that is <em>deterministic but unpredictable</em>: sensitive to initial
      conditions, bounded but never periodic, tracing a fractal structure through phase space.
    </p>
    <div class="law">
      dx/dt = σ(y − x)<br>
      dy/dt = x(ρ − z) − y<br>
      dz/dt = xy − βz
    </div>
    <p>
      This is why it belongs here. The attractor is a dissipative system operating far from
      equilibrium — exactly what Freed's Law describes. The system burns entropy continuously
      (dS/dt &gt; 0) while maintaining coherent geometric structure. Order and disorder
      coexist at the critical boundary. The two lobes are the two basins — the system
      cannot settle, cannot escape, and cannot repeat. It is, structurally, a reasoning
      substrate: always processing, never finishing, never the same twice.
    </p>
  </div>

</div>
<script>
// ── Lorenz Attractor ──────────────────────────────────────────────────────────
// RK4 integration. Orthographic projection with slow Y-axis rotation.

const SIGMA = 10, RHO = 28, BETA = 8/3;
const DT    = 0.005;

let state   = { x: 0.1, y: 0, z: 0 };
let trail   = [];
let angle   = 0;
let t       = 0;
let running = true;
let animId  = null;
let stepsPerFrame = 6;

const canvas = document.getElementById('canvas');
const ctx    = canvas.getContext('2d');

function initCanvas() {
  const wrap = canvas.parentElement;
  const w    = wrap.clientWidth;
  canvas.width  = w;
  canvas.height = Math.round(w * 0.56);
}

function lorenz(x, y, z) {
  return {
    dx: SIGMA * (y - x),
    dy: x * (RHO - z) - y,
    dz: x * y - BETA * z
  };
}

function rk4Step(s) {
  const k1 = lorenz(s.x, s.y, s.z);
  const k2 = lorenz(s.x + k1.dx*DT/2, s.y + k1.dy*DT/2, s.z + k1.dz*DT/2);
  const k3 = lorenz(s.x + k2.dx*DT/2, s.y + k2.dy*DT/2, s.z + k2.dz*DT/2);
  const k4 = lorenz(s.x + k3.dx*DT,   s.y + k3.dy*DT,   s.z + k3.dz*DT);
  return {
    x: s.x + (k1.dx + 2*k2.dx + 2*k3.dx + k4.dx) * DT/6,
    y: s.y + (k1.dy + 2*k2.dy + 2*k3.dy + k4.dy) * DT/6,
    z: s.z + (k1.dz + 2*k2.dz + 2*k3.dz + k4.dz) * DT/6
  };
}

function project(x, y, z) {
  // Rotate around Y axis
  const cosA = Math.cos(angle), sinA = Math.sin(angle);
  const rx = x * cosA + z * sinA;
  const ry = y;
  const rz = -x * sinA + z * cosA;
  // Orthographic: map to canvas coords
  const cx = canvas.width  / 2;
  const cy = canvas.height / 2;
  const scale = canvas.width / 90;
  return {
    sx: cx + rx * scale,
    sy: cy - (ry - 25) * scale,   // centre vertically (attractor sits ~z=25)
    depth: rz
  };
}

function draw() {
  const maxTrail = parseInt(document.getElementById('trail').value);
  const visible  = trail.slice(-maxTrail);
  const n        = visible.length;

  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = '#080808';
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  if (n < 2) return;

  for (let i = 1; i < n; i++) {
    const frac  = i / n;                        // 0=oldest, 1=newest
    const alpha = Math.pow(frac, 1.4);          // age fade
    // Colour: deep crimson → bright red → near-white at tip
    const r = Math.round(80  + 175 * frac);
    const g = Math.round(0   + 60  * frac);
    const b = Math.round(0   + 40  * frac);
    ctx.strokeStyle = `rgba(${r},${g},${b},${alpha})`;
    ctx.lineWidth   = 0.8 + frac * 1.2;
    ctx.beginPath();
    const p0 = project(visible[i-1].x, visible[i-1].y, visible[i-1].z);
    const p1 = project(visible[i].x,   visible[i].y,   visible[i].z);
    ctx.moveTo(p0.sx, p0.sy);
    ctx.lineTo(p1.sx, p1.sy);
    ctx.stroke();
  }
}

function updateStats() {
  document.getElementById('stat-t').textContent   = t.toFixed(3);
  document.getElementById('stat-xyz').textContent =
    `${state.x.toFixed(3)} · ${state.y.toFixed(3)} · ${state.z.toFixed(3)}`;
}

function loop() {
  stepsPerFrame = parseInt(document.getElementById('speed').value);
  for (let i = 0; i < stepsPerFrame; i++) {
    state = rk4Step(state);
    trail.push({ x: state.x, y: state.y, z: state.z });
    t += DT;
  }
  // Trim trail to max
  const maxTrail = parseInt(document.getElementById('trail').value);
  if (trail.length > maxTrail + 200) trail.splice(0, trail.length - maxTrail);

  angle += 0.003;   // slow rotation ~0.17°/frame
  draw();
  updateStats();
  if (running) animId = requestAnimationFrame(loop);
}

function togglePlay() {
  running = !running;
  document.getElementById('btn-play').textContent = running ? '⏸ PAUSE' : '▶ PLAY';
  if (running) animId = requestAnimationFrame(loop);
}

function resetSim() {
  cancelAnimationFrame(animId);
  state  = { x: 0.1, y: 0, z: 0 };
  trail  = [];
  angle  = 0;
  t      = 0;
  running = true;
  document.getElementById('btn-play').textContent = '⏸ PAUSE';
  animId = requestAnimationFrame(loop);
}

window.addEventListener('resize', () => { initCanvas(); draw(); });
initCanvas();
animId = requestAnimationFrame(loop);
</script>
</body>
</html>
"""
    (DOCS_DIR / "lorenz.html").write_text(html, encoding="utf-8")


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

    # Load promotion candidates from state
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
    --bg:      #ffffff;
    --surface: #f7f7f5;
    --border:  #e0ddd8;
    --accent:  #b91c1c;
    --green:   #16a34a;
    --amber:   #b45309;
    --blue:    #1d4ed8;
    --red:     #dc2626;
    --text:    #111111;
    --muted:   #374151;
    --mono:    'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
    --serif:   'Cormorant Garamond', 'Palatino Linotype', Georgia, serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body {
    height: 100vh;
    overflow: hidden;
    background: var(--bg);
    color: var(--text);
    font-family: var(--serif);
    font-weight: 300;
    font-size: 15px;
    line-height: 1.6;
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* ── HUD shell ── */
  .hud-shell {
    display: flex;
    flex-direction: column;
    height: 100vh;
  }
  .hud-top {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 0.55rem 1.4rem;
    border-bottom: 1px solid #000;
    flex-shrink: 0;
    flex-wrap: wrap;
  }
  .hud-title {
    font-family: var(--serif);
    font-weight: 300;
    font-size: 1.5rem;
    letter-spacing: 0.18em;
    color: var(--accent);
  }
  .hud-sub {
    font-family: var(--serif);
    font-weight: 300;
    font-size: 0.82rem;
    color: var(--muted);
  }
  .hud-top-divider { color: var(--border); }
  .daemon-status {
    margin-left: auto; display: flex; align-items: center; gap: 0.5rem;
    font-family: var(--mono); font-size: 0.62rem; color: var(--muted);
  }
  .daemon-phase {
    font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase;
    padding: 0.15rem 0.4rem; border: 1px solid currentColor;
  }
  .daemon-phase.idle       { color: var(--green); }
  .daemon-phase.perceive   { color: var(--blue); }
  .daemon-phase.represent  { color: var(--accent); }
  .daemon-phase.compress   { color: var(--amber); }
  .daemon-phase.predict    { color: var(--amber); }
  .daemon-phase.compare    { color: var(--accent); }
  .daemon-phase.adjust     { color: var(--muted); }
  .daemon-phase.repeat     { color: var(--green); }
  .daemon-phase.pre-audit  { color: var(--muted); }
  /* Panel subtitle — kernel step whisper */
  .panel-subtitle {
    font-family: var(--mono); font-size: 0.55rem; letter-spacing: 0.13em;
    color: var(--muted); text-align: center; text-transform: uppercase;
    opacity: 0.65; margin-top: 0.15rem; padding-bottom: 0.35rem;
    border-bottom: 1px solid var(--border); flex-shrink: 0;
  }
  /* Kernel chain progress */
  .kernel-chain {
    display: flex; align-items: center; gap: 0.3rem; margin-left: auto;
    font-family: var(--mono); font-size: 0.58rem; letter-spacing: 0.07em;
  }
  .kstep { color: var(--border); transition: color 0.3s; text-transform: uppercase; }
  .kstep.kstep-active { color: var(--accent); font-weight: 600; }
  .karrow { color: var(--border); font-size: 0.5rem; }
  .daemon-detail { color: var(--muted); max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .hud-grid {
    display: grid;
    grid-template-columns: 30fr 40fr 30fr;
    flex: 1;
    overflow: hidden;
    min-height: 0;
  }
  .hud-panel {
    overflow-y: auto;
    padding: 0.9rem 1.1rem;
    border-right: 1px solid #000;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  .hud-panel:last-child { border-right: none; }
  .hud-footer {
    border-top: 1px solid var(--border);
    padding: 0.35rem 1.4rem;
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-shrink: 0;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  .panel-title {
    font-family: var(--mono);
    font-size: 0.78rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--text);
    text-align: center;
    padding-bottom: 0.1rem;
    flex-shrink: 0;
  }

  /* Panel blocks — law and formulations */
  .law {
    padding: 0.6rem 0.85rem;
    background: var(--surface);
    border: 1px solid var(--accent);
    font-family: var(--serif);
    font-weight: 300;
    font-size: 0.95rem;
    font-style: italic;
  }
  .law .label { font-family: var(--mono); font-style: normal; color: var(--muted); font-size: 0.62rem; margin-bottom: 0.25rem; letter-spacing: 0.1em; text-transform: uppercase; }
  .formulations {
    padding: 0.6rem 0.85rem;
    background: var(--surface);
    border-left: 3px solid var(--border);
    font-family: var(--mono);
    font-size: 0.72rem;
  }
  .formulations .label { color: var(--muted); font-size: 0.62rem; margin-bottom: 0.4rem; letter-spacing: 0.1em; text-transform: uppercase; }
  .form-chain { color: var(--text); margin-bottom: 0.5rem; font-family: var(--serif); font-style: italic; font-size: 0.82rem; font-weight: 300; }
  .form-chain .arrow { color: var(--accent); margin: 0 0.25rem; font-style: normal; }
  .form-row { display: flex; align-items: baseline; gap: 0.6rem; margin-top: 0.28rem; line-height: 1.5; }
  .form-tag { color: var(--muted); font-size: 0.6rem; letter-spacing: 0.07em; text-transform: uppercase; min-width: 6rem; flex-shrink: 0; }
  .form-expr { color: var(--text); font-size: 0.7rem; }

  /* Pulse indicator — top bar compact circle */
  .pulse {
    display: inline-block;
    width: 22px; height: 22px;
    border-radius: 50%;
    border: 2px solid var(--accent);
    background: transparent;
    flex-shrink: 0;
    animation: pulse 2s infinite;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.2; }
  }
  @keyframes flash-update {
    0%   { background: var(--accent); color: #fff; border-radius: 2px; }
    100% { background: transparent;   color: inherit; }
  }
  .flash-val { animation: flash-update 0.7s ease-out; padding: 0 3px; margin: 0 -3px; }
  /* State panel */
  .state-grid { display: flex; flex-direction: column; gap: 0; margin-bottom: 0.5rem; border: 1px solid var(--green); padding: 0 0.6rem; }
  .state-cell {
    display: flex; justify-content: space-between; align-items: baseline;
    padding: 0.32rem 0;
    border-bottom: 1px solid var(--border);
  }
  .state-cell:last-child { border-bottom: none; }
  .state-cell .label { font-family: var(--mono); color: var(--muted); font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.08em; }
  .state-cell .value { font-family: var(--mono); font-size: 0.82rem; color: var(--text); }
  .state-cell .value.accent { color: var(--accent); font-weight: 600; }
  .state-cell .value.green  { color: var(--green);  font-weight: 600; }

  /* Section */
  .section { margin-bottom: 2.5rem; }
  .section-title {
    font-family: var(--mono);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem;
    margin-bottom: 1rem;
  }
  /* Collapsible sections */
  details.section { margin-bottom: 2.5rem; }
  /* Unified summary style — shared by right-column sections and center ob-groups */
  details.section > summary,
  details.ob-group > summary.ob-group-title {
    font-family: var(--mono);
    font-size: 0.68rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem;
    margin-bottom: 1rem;
    cursor: pointer;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    user-select: none;
  }
  details.section > summary::-webkit-details-marker,
  details.ob-group > summary.ob-group-title::-webkit-details-marker { display: none; }
  /* Triangle on the left */
  details.section > summary::before,
  details.ob-group > summary.ob-group-title::before {
    content: '▶';
    font-size: 0.55rem;
    opacity: 0.5;
    transition: transform 0.15s;
    flex-shrink: 0;
  }
  details.section[open] > summary::before,
  details.ob-group[open] > summary.ob-group-title::before { transform: rotate(90deg); opacity: 1; }
  details.section > summary::after,
  details.ob-group > summary.ob-group-title::after {
    content: ' (click to open)'; font-size: 0.58rem; color: var(--muted); opacity: 0.7; font-weight: 400; letter-spacing: 0.04em;
  }
  details.section[open] > summary::after,
  details.ob-group[open] > summary.ob-group-title::after { content: ''; }
  details.section > summary:hover,
  details.ob-group > summary.ob-group-title:hover { color: var(--text); }

  /* Obligation cards */
  .obligation {
    border: 1px solid var(--border);
    background: var(--surface);
    padding: 0.9rem 1rem;
    margin-bottom: 0.6rem;
    position: relative;
  }
  .obligation .ob-header {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    margin-bottom: 0.4rem;
  }
  .ob-id { font-family: var(--mono); color: var(--accent); font-size: 0.8rem; }
  .ob-status {
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.1rem 0.4rem;
    border-radius: 2px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .ob-status.open     { background: #fffbeb; color: var(--amber); border: 1px solid var(--amber); }
  .ob-status.partial  { background: #eff6ff; color: var(--blue);  border: 1px solid var(--blue);  }
  .ob-status.resolved { background: #f0fdf4; color: var(--green); border: 1px solid var(--green); }
  .ob-priority {
    position: absolute; top: 0.55rem; right: 0.75rem;
    font-family: var(--mono); font-size: 0.68rem; color: var(--muted);
    letter-spacing: 0.05em; line-height: 1;
  }
  .ob-statement { font-family: var(--serif); font-weight: 300; font-size: 0.88rem; margin-bottom: 0.5rem; }
  .ob-progress  { font-family: var(--serif); font-weight: 300; font-size: 0.82rem; color: var(--muted); border-left: 2px solid var(--border); padding-left: 0.6rem; }
  .ob-date { font-family: var(--mono); font-size: 0.65rem; color: var(--muted); margin-top: 0.4rem; }
  /* Obligation sub-groups (Open / Partial) */
  details.ob-group { margin-bottom: 0.7rem; }
  .ob-group-count {
    font-family: var(--mono); font-size: 0.62rem;
    color: var(--bg); background: var(--muted);
    padding: 0.05rem 0.38rem; border-radius: 2px; margin-left: auto;
  }
  details.ob-group[open] .ob-group-count { background: var(--text); }

  /* Cycle log */
  .cycle-entry {
    border-left: 2px solid var(--border);
    padding-left: 0.9rem;
    margin-bottom: 1rem;
  }
  .cycle-entry:first-child { border-left-color: var(--accent); }
  .cycle-meta { font-family: var(--mono); color: var(--muted); font-size: 0.68rem; margin-bottom: 0.3rem; }
  .cycle-feed { margin-top: 0.4rem; }
  .cycle-feed-item { font-size: 0.95rem; padding: 0.3rem 0; border-bottom: 1px solid var(--border); }
  .cycle-feed-item .feed-title { font-family: var(--serif); font-weight: 300; color: var(--accent); }
  .cycle-feed-item .feed-compress { font-family: var(--serif); font-weight: 300; font-style: italic; color: var(--muted); font-size: 0.9rem; margin-top: 0.1rem; }
  .cycle-resolve { font-family: var(--serif); font-weight: 300; margin-top: 0.5rem; font-size: 0.95rem; color: var(--text); }

  /* Kernel diagram */
  .kernel {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0;
    margin: 1rem 0;
  }
  .kernel-step {
    font-family: var(--serif);
    font-weight: 300;
    letter-spacing: 0.06em;
    padding: 0.35rem 0.8rem;
    background: var(--surface);
    border: 1px solid var(--border);
    font-size: 0.95rem;
    color: var(--accent);
  }
  .kernel-arrow { font-family: var(--mono); color: var(--muted); padding: 0 0.2rem; font-size: 0.75rem; }

  /* Project nodes */
  .node {
    border: 1px solid var(--border);
    background: var(--surface);
    padding: 1rem;
    margin-bottom: 0.8rem;
  }
  .node-header { display: flex; gap: 0.75rem; align-items: baseline; margin-bottom: 0.5rem; flex-wrap: wrap; }
  .node-title  { font-family: var(--serif); font-weight: 300; color: var(--accent); font-size: 1.15rem; }
  .node-gen    { font-family: var(--mono); color: var(--muted); font-size: 0.67rem; }
  .node-summary { font-family: var(--serif); font-weight: 300; font-size: 0.98rem; margin-bottom: 0.5rem; }
  .node-compress {
    font-family: var(--serif); font-weight: 300; font-style: italic;
    font-size: 0.98rem; border-left: 2px solid var(--accent);
    padding-left: 0.6rem; color: var(--text); margin-bottom: 0.5rem;
  }
  .node-next   { font-family: var(--serif); font-weight: 300; font-size: 0.92rem; color: var(--muted); margin-bottom: 0.5rem; }
  .node-tags   { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-top: 0.4rem; }
  .node-tag    {
    font-size: 0.65rem; padding: 0.1rem 0.4rem;
    background: var(--surface); border: 1px solid var(--border);
    color: var(--muted); border-radius: 2px;
  }
  .node-tag.inv  { border-color: var(--accent); color: var(--accent); }
  .node-tag.ob   { border-color: var(--amber);  color: var(--amber);  }
  .node-council  { font-size: 0.72rem; color: var(--muted); margin-top: 0.3rem; }
  .node.drifting { border-left: 3px solid var(--amber); }
  .node-drift-badge { font-family: var(--mono); font-size: 0.62rem; color: var(--amber);
    margin-top: 0.25rem; letter-spacing: 0.05em; }

  /* Genome promotion queue */
  .promo-candidate { padding: 0.5rem 0; border-bottom: 1px solid var(--border); }
  .promo-candidate:last-child { border-bottom: none; }
  .promo-text  { font-family: var(--serif); font-weight: 300; font-size: 0.95rem; }
  .promo-meta  { font-family: var(--mono); font-size: 0.62rem; color: var(--muted); margin-top: 0.2rem; }
  .promo-count { color: var(--green); font-weight: 500; }

  /* Genome symbols */
  .symbol {
    border: 1px solid var(--border);
    background: var(--surface);
    padding: 0.9rem 1rem;
    margin-bottom: 0.6rem;
  }
  .symbol-header {
    display: flex; align-items: baseline; gap: 0.75rem;
    margin-bottom: 0.5rem; flex-wrap: wrap;
  }
  .symbol-name { font-family: var(--serif); font-weight: 300; color: var(--accent); font-size: 1.1rem; letter-spacing: 0.04em; }
  .symbol-recurrence { font-family: var(--mono); font-size: 0.68rem; color: var(--muted); display: flex; align-items: center; gap: 0.4rem; }
  .symbol-bar-track {
    width: 80px; height: 6px;
    background: var(--border); border-radius: 3px; overflow: hidden; display: inline-block;
  }
  .symbol-bar-fill { height: 100%; border-radius: 3px; background: var(--accent); }
  .symbol-badge {
    font-size: 0.6rem; padding: 0.1rem 0.35rem;
    border-radius: 2px; text-transform: uppercase; letter-spacing: 0.06em;
  }
  .symbol-badge.new-badge { background: #f0fdf4; color: var(--green); border: 1px solid var(--green); }
  .symbol-canonical { font-family: var(--serif); font-weight: 300; font-size: 1rem; margin-bottom: 0.5rem; }
  .symbol-role {
    font-family: var(--serif); font-weight: 300; font-style: italic;
    font-size: 0.95rem; border-left: 2px solid var(--accent);
    padding-left: 0.6rem; color: var(--text); margin-bottom: 0.5rem;
  }
  .symbol-confirmed { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-bottom: 0.4rem; }
  .symbol-confirmed-tag {
    font-size: 0.62rem; padding: 0.1rem 0.35rem;
    background: var(--surface); border: 1px solid var(--border);
    color: var(--muted); border-radius: 2px;
  }
  .symbol-drift {
    font-size: 0.72rem; color: var(--muted);
    border-left: 2px solid var(--amber); padding-left: 0.5rem; margin-top: 0.3rem;
  }
  .symbol-drift-label { color: var(--amber); font-size: 0.65rem; margin-bottom: 0.2rem; }

  /* Footer */
  .footer {
    border-top: 1px solid var(--border);
    padding-top: 1rem;
    margin-top: 3rem;
    color: var(--muted);
    font-family: var(--serif);
    font-weight: 300;
    font-size: 0.95rem;
  }
  .footer .architect { color: var(--text); }

  /* Speak bar */
  .speak-bar {
    display: flex; flex-direction: column; gap: 0.45rem;
    padding: 0.5rem 0.6rem; border: 1px solid #000; margin-top: auto;
  }
  /* Character voice buttons */
  .char-btns { display: flex; flex-wrap: wrap; gap: 0.3rem; }
  .char-btn {
    font-family: var(--mono); font-size: 0.63rem; padding: 0.2rem 0.55rem;
    background: transparent; border: 1px solid var(--border); color: var(--muted);
    cursor: pointer; letter-spacing: 0.06em; transition: all 0.12s;
  }
  .char-btn:hover { opacity: 0.8; }
  .char-btn.char-active { filter: brightness(0.75); }
  /* Individual voice colors */
  .char-btn[data-voice="Boing"]     { border-color: #65a30d; color: #65a30d; }
  .char-btn[data-voice="Fred"]      { border-color: #c9a87c; color: #c9a87c; }
  .char-btn[data-voice="Zarvox"]    { border-color: #7c3aed; color: #7c3aed; }
  .char-btn[data-voice="Superstar"] { border-color: #0ea5e9; color: #0ea5e9; }
  .char-btn[data-voice="Trinoids"]  { border-color: transparent; color: #fff;
    background: linear-gradient(90deg,#f472b6,#818cf8,#34d399,#fbbf24,#f472b6);
    background-size: 300% 100%; animation: holo 3s linear infinite; }
  @keyframes holo {
    0%   { background-position: 0% 50%; }
    100% { background-position: 300% 50%; }
  }
  /* Main row: speak btn + selects + rate */
  .speak-main-row {
    display: flex; align-items: center; flex-wrap: wrap; gap: 0.45rem;
  }
  .speak-btn {
    display: inline-flex; align-items: center; gap: 0.5rem;
    padding: 0.4rem 1.1rem; background: transparent;
    border: 1px solid var(--accent); color: var(--accent);
    font-family: var(--mono); font-size: 0.72rem; letter-spacing: 0.1em;
    cursor: pointer; transition: background 0.15s, color 0.15s;
  }
  .speak-btn:hover { background: var(--accent); color: var(--bg); }
  .speak-btn.speaking { background: var(--accent); color: var(--bg); }
  .speak-btn.speaking:hover { background: transparent; color: var(--accent); }
  .speak-status { font-size: 0.7rem; color: var(--muted); }
  .voice-label {
    font-family: var(--mono); font-size: 0.62rem; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.1em;
  }
  .voice-select {
    background: var(--surface); border: 1px solid var(--border);
    color: var(--text); font-family: var(--mono); font-size: 0.68rem;
    padding: 0.2rem 0.4rem; cursor: pointer; max-width: 180px;
  }
  .voice-select:focus { outline: 1px solid var(--accent); }
  input[type=range].rate-slider { accent-color: var(--accent); width: 70px; }

  /* Loading state */
  .loading { color: var(--muted); font-style: italic; }
  .error   { color: var(--red); }

  /* ── Mobile ────────────────────────────────────────────────────────────── */
  @media (max-width: 768px) {
    .hud-shell { height: auto; min-height: 100vh; }
    .hud-grid {
      grid-template-columns: 1fr;
      overflow: visible;
      flex: none;
    }
    .hud-panel {
      overflow-y: visible;
      min-height: auto;
      border-right: none;
      border-bottom: 1px solid #000;
    }
    .hud-panel:last-child { border-bottom: none; }
    .hud-top { padding: 0.6rem 0.9rem; gap: 0.6rem; }
    .hud-title { font-size: 1.15rem; }
    .speak-bar { margin-top: 0; }
    .speak-main-row { flex-wrap: wrap; }
    .state-grid { border: 1px solid var(--green); }
  }
</style>
</head>
<body>
<div class="hud-shell">

  <!-- Top bar -->
  <div class="hud-top">
    <span class="pulse" id="pulse"></span>
    <span class="hud-title">RSA</span>
    <span class="hud-sub">a bootstrap protocol for epistemic recursion</span>
    <div class="kernel-chain" id="kernel-chain">
      <span class="kstep" data-phases="perceive">PERCEIVE</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="represent">REPRESENT</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="compress">COMPRESS</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="predict">PREDICT</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="compare">COMPARE</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="adjust">ADJUST</span>
      <span class="karrow">→</span>
      <span class="kstep" data-phases="repeat">REPEAT</span>
    </div>
    <div class="daemon-status">
      <span class="daemon-phase idle" id="daemon-phase">IDLE</span>
      <span class="daemon-detail" id="daemon-detail">—</span>
    </div>
  </div>

  <!-- Main 3-panel grid -->
  <div class="hud-grid">

    <!-- LEFT PANEL: argument + law + kernel + speak -->
    <div class="hud-panel">

      <div class="panel-title">The Argument</div>
      <div class="panel-subtitle">Perceive · Represent</div>

      <div class="formulations">
        <div class="label">Reasoning Substrate Argument</div>
        <div class="form-chain">
          Reasoning is real
          <span class="arrow">→</span>
          Causal structure must exist
          <span class="arrow">→</span>
          Something physical exists
        </div>
        <div class="form-chain" style="font-size:0.75rem;font-style:normal;margin-bottom:0.6rem">
          RSA &nbsp;<span class="arrow">≡</span>&nbsp; Recursive Semantic Alignment
        </div>
        <div class="form-row"><span class="form-tag">Freed's Law</span>  <span class="form-expr">∃R(t) → ∃M₀ : dS(M<sub>R</sub>,t)/dt &gt; 0</span></div>
        <div class="form-row"><span class="form-tag">First-order</span>  <span class="form-expr">∀t[ R(t) → ∃m( Physical(m) ∧ Substrate(m,R,t) )]</span></div>
        <div class="form-row"><span class="form-tag">Modal</span>        <span class="form-expr">◇R(t) → □∃M[ Entropic(M) ∧ Runs(M,R) ]</span></div>
        <div class="form-row"><span class="form-tag">Fixed point</span>  <span class="form-expr">R[R] = R</span></div>
        <div class="form-row"><span class="form-tag">Landauer</span>     <span class="form-expr">W ≥ kT ln 2 &nbsp;per bit erased</span></div>
        <div class="form-row"><span class="form-tag">Category</span>     <span class="form-expr">ε ∘ ε = ε &nbsp;(idempotent on Process)</span></div>
        <div class="form-row"><span class="form-tag">Gödel</span>        <span class="form-expr">PA ⊬ ∃x[ Compute(x) ∧ ¬Physical(x) ]</span></div>
        <div class="form-row"><span class="form-tag">Mandelbrot</span>   <span class="form-expr">z<sub>n+1</sub> = z<sub>n</sub>² + c &nbsp;→&nbsp; R[R]=R at boundary (γ=1)</span></div>
      </div>

      <div class="law">
        <div class="label">Freed's Law</div>
        ∃R(t) → ∃M₀ : dS(M<sub>R</sub>,t)/dt &gt; 0
        <br>
        <span style="color:var(--muted);font-size:0.78rem">
          To reason is to exist physically. To think is to burn. To be is to be built.
        </span>
        <br>
        <a href="game_of_life.html" style="font-family:var(--mono);font-size:0.68rem;letter-spacing:0.06em;color:var(--accent)">Freed's Law Simulation (click here →)</a>
        <br>
        <a href="lorenz.html" style="font-family:var(--mono);font-size:0.68rem;letter-spacing:0.06em;color:var(--accent)">Lorenz Attractor (click here →)</a>
      </div>

      <div class="panel-title" style="margin-top:0.2rem">RSA Kernel — The Process</div>
      <div>
        <div class="kernel">
          <span class="kernel-step">Perceive</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Represent</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Predict</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Compare</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Adjust</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Compress</span><span class="kernel-arrow">→</span>
          <span class="kernel-step">Repeat</span>
        </div>
        <div style="color:var(--muted);font-size:0.72rem;margin-top:0.4rem;font-family:var(--mono)">
          R[R] = R &nbsp;·&nbsp; γ = 1 &nbsp;·&nbsp; Only Processes Exist (MCPM)
        </div>
      </div>

      <!-- Speak bar — pushed to bottom by margin-top:auto on .speak-bar -->
      <div class="speak-bar">
        <!-- Character voice one-click buttons -->
        <div class="char-btns">
          <button class="char-btn" data-voice="Boing"     onclick="speakWithVoice('Boing')">▶ BOING</button>
          <button class="char-btn" data-voice="Fred"      onclick="speakWithVoice('Fred')">▶ STEPHEN HAWKING</button>
          <button class="char-btn" data-voice="Trinoids"  onclick="speakWithVoice('Trinoids')">▶ TRINOIDS</button>
          <button class="char-btn" data-voice="Zarvox"    onclick="speakWithVoice('Zarvox')">▶ ZARVOX</button>
          <button class="char-btn" data-voice="Superstar" onclick="speakWithVoice('Superstar')">▶ SUPERSTAR</button>
        </div>
        <!-- Main row -->
        <div class="speak-main-row">
          <button class="speak-btn" id="speak-btn" onclick="toggleSpeak()">▶ SPEAK DIGEST</button>
          <span class="speak-status" id="speak-status"></span>
          <span class="voice-label" style="color:var(--accent)">VOICE</span>
          <select id="voice-select" class="voice-select" onchange="saveVoicePref()" style="border-color:var(--accent);color:var(--accent)">
            <option value="">Loading...</option>
          </select>
          <span class="voice-label">LANGUAGES</span>
          <select id="voice-lang" class="voice-select" onchange="syncVoiceFrom('voice-lang')">
            <option value="">—</option>
          </select>
          <span class="voice-label">RATE</span>
          <input type="range" class="rate-slider" id="rate-slider"
            min="0.5" max="1.4" step="0.05" value="1.10"
            oninput="saveVoicePref(); document.getElementById('rate-val').textContent=parseFloat(this.value).toFixed(2)">
          <span id="rate-val">1.10</span>
        </div>
      </div>

    </div><!-- /left panel -->

    <!-- CENTER PANEL: open obligations -->
    <div class="hud-panel">
      <div class="panel-title">Open Obligations</div>
      <div class="panel-subtitle">Predict · Compare · Adjust</div>
      <div id="obligations-open" class="loading">Loading...</div>
    </div><!-- /center panel -->

    <!-- RIGHT PANEL: state + collapsibles -->
    <div class="hud-panel">
      <div class="panel-title">Live State</div>
      <div class="panel-subtitle">Compress</div>
      <div class="state-grid" id="state-grid">
        <div class="state-cell"><div class="label">Status</div><div class="value loading">Loading...</div></div>
      </div>
      <details class="section">
        <summary>Resolved Obligations — Track Record</summary>
        <div id="obligations-resolved" class="loading">Loading...</div>
      </details>

      <details class="section">
        <summary>Genome Promotion Queue — Invariants Awaiting Elevation</summary>
        <div id="promo-queue">''' + promo_html + r'''</div>
      </details>

      <details class="section">
        <summary>Project Nodes — Framework Compressions</summary>
        <div id="projects">''' + (projects_html or '<div class="loading">No nodes yet.</div>') + r'''</div>
      </details>

      <details class="section">
        <summary>Genome Registry — Confirmed Symbols</summary>
        <div id="symbols" class="loading">Loading...</div>
      </details>

      <details class="section">
        <summary>Recent Recursions — What FREED Processed</summary>
        <div id="cycles" class="loading">Loading...</div>
      </details>

    </div><!-- /right panel -->

  </div><!-- /hud-grid -->

  <!-- Footer bar -->
  <div class="hud-footer">
    <span>Architect: David Harry Freed — mail carrier, Olney Maryland &nbsp;·&nbsp; v1 Apr 2025 → present</span>
    <span>FREED — autonomous science daemon &nbsp;·&nbsp; Generated <span id="generated-at">—</span></span>
  </div>

</div><!-- /hud-shell -->

<script>
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
    month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit', timeZoneName: 'short'
  });
}

// ── Render state grid ─────────────────────────────────────────────────────────

let _lastStateValues = {};

function renderState(s) {
  if (!s) { document.getElementById('state-grid').innerHTML = '<div class="state-cell"><div class="value error">Unavailable</div></div>'; return; }
  const cells = [
    { label: 'Generation',  value: s.generation,                        cls: 'accent' },
    { label: 'Coherence',   value: s.coherence,                         cls: s.coherence >= 1 ? 'error' : 'green' },
    { label: 'Cycle Count', value: s.cycle_count },
    { label: 'Topology',    value: (s.topology||'').replace(/_/g,' ') },
    { label: 'Debt Ratio',  value: s.debt_ratio },
    { label: 'Last Recursion', value: ts(s.last_cycle) },
  ];
  const isFirstLoad = Object.keys(_lastStateValues).length === 0;
  document.getElementById('state-grid').innerHTML = cells.map(c =>
    `<div class="state-cell" data-key="${c.label}">
      <div class="label">${c.label}</div>
      <div class="value ${c.cls||''}">${c.value||'—'}</div>
    </div>`
  ).join('');
  // Flash values that changed since last render (skip first load)
  if (!isFirstLoad) {
    cells.forEach(c => {
      const cur = String(c.value ?? '—');
      if (_lastStateValues[c.label] !== cur) {
        const el = document.querySelector(`[data-key="${c.label}"] .value`);
        if (el) {
          el.classList.remove('flash-val');
          void el.offsetWidth; // reflow to restart animation if already running
          el.classList.add('flash-val');
          el.addEventListener('animationend', () => el.classList.remove('flash-val'), {once: true});
        }
      }
    });
  }
  cells.forEach(c => { _lastStateValues[c.label] = String(c.value ?? '—'); });
  document.getElementById('generated-at').textContent = ts(s.generated);
}

// ── Render obligations ────────────────────────────────────────────────────────

const STATUS_COLOR = {
  open:     'var(--amber)',
  partial:  'var(--blue)',
  resolved: 'var(--green)',
};

function _priorityCarats(p) {
  const map = { critical: '^^^', high: '^^', medium: '^', normal: '', low: '' };
  const carats = map[(p||'').toLowerCase()] ?? '';
  return carats ? `<span class="ob-priority" title="${p} priority">Priority ${carats}</span>` : '';
}

function _linkify(text) {
  return (text||'').replace(
    /\b(https?:\/\/\S+|(?:www\.|osf\.io|github\.com|arxiv\.org)\S*)/g,
    url => {
      const href = url.startsWith('http') ? url : 'https://' + url;
      return `<a href="${href}" target="_blank" rel="noopener" style="color:var(--accent)">click this hyperlink</a>`;
    }
  );
}

function renderObligation(o) {
  const borderColor = STATUS_COLOR[o.status] || 'var(--border)';
  return `<div class="obligation" style="border-left: 3px solid ${borderColor}">
    ${_priorityCarats(o.priority)}
    <div class="ob-header">
      <span class="ob-id">${o.id}</span>
    </div>
    <div class="ob-statement">${_linkify(o.statement)}</div>
    ${o.progress ? `<div class="ob-progress">${_linkify(o.progress)}</div>` : ''}
    <div class="ob-date">Created ${o.created||'—'}${o.resolved ? ' · Resolved '+o.resolved : ''}</div>
  </div>`;
}

function _collapseSection(label, cards, openByDefault) {
  const inner = cards.length
    ? cards.map(renderObligation).join('')
    : `<div style="color:var(--muted);font-style:italic;font-size:0.9rem">None.</div>`;
  const attr = openByDefault ? ' open' : '';
  return `<details class="ob-group"${attr}>
    <summary class="ob-group-title">${label} <span class="ob-group-count">${cards.length}</span></summary>
    <div class="ob-group-body">${inner}</div>
  </details>`;
}

function _setBadge(detailsId, count) {
  // Inject or update a count badge in the nearest parent details > summary
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
  // A feed is a null/rejected result — correct behaviour, but not display-worthy
  const c = (f.compress || '').toUpperCase();
  return c.includes('REJECTED') || c.includes('UNMETABOLIZABLE') ||
         c.includes('NULL FEED') || c.includes('GENOME-EXTERIOR');
}

function renderCycles(cycles) {
  if (!cycles || !cycles.length) {
    document.getElementById('cycles').innerHTML = '<div class="loading">No cycles recorded yet.</div>';
    return;
  }
  const recent = [...cycles].reverse().slice(0, 10);
  document.getElementById('cycles').innerHTML = recent.map((c, i) => {
    const allFeeds   = c.feed || [];
    const goodFeeds  = allFeeds.filter(f => !_feedRejected(f));
    const nullCount  = allFeeds.length - goodFeeds.length;

    const feedHtml = goodFeeds.map(f =>
      `<div class="cycle-feed-item">
        <div class="feed-title">${f.title||'?'}</div>
        ${f.compress ? `<div class="feed-compress">↳ ${f.compress}</div>` : ''}
      </div>`
    ).join('');

    const nullNote = nullCount > 0
      ? `<div style="font-family:var(--mono);font-size:0.65rem;color:var(--muted);margin-top:0.3rem">${nullCount} input${nullCount>1?'s':''} correctly rejected — no genome movement warranted</div>`
      : '';

    const res = c.resolve||{};
    const resolveHtml = res.obligation
      ? `<div class="cycle-resolve">
          RESOLVE → ${res.obligation}: ${res.compress||''}
          ${res.resolved ? ' <span style="color:var(--green)">[RESOLVED]</span>' : ''}
        </div>`
      : '';

    return `<div class="cycle-entry">
      <div class="cycle-meta">
        Cycle ${c.cycle||'?'} · Gen ${c.generation||'?'} · ${ts(c.timestamp)}
        ${c.coherence ? ` · coherence ${c.coherence}` : ''}
      </div>
      ${(c.sweep||{}).input_count > 0 ? `<div style="font-family:var(--mono);font-size:0.72rem;color:var(--accent);margin-bottom:0.3rem">↓ ${c.sweep.input_count} new paper(s) ingested</div>` : ''}
      <div class="cycle-feed">${feedHtml}${nullNote}</div>
      ${resolveHtml}
    </div>`;
  }).join('');
}

// ── Speak digest ─────────────────────────────────────────────────────────────

let _speaking = false;
let _loadedState = null, _loadedObligs = null, _loadedProjects = [], _loadedSymbols = null;

function cleanText(s) {
  return (s || '')
    .replace(/\*{1,2}|_{1,2}|`{1,3}/g, '')
    .replace(/[→↳·]/g, '.')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildDigest() {
  const chunks = [];

  // State
  const s = _loadedState;
  if (s) {
    chunks.push(`FREED. Generation ${s.generation}. Coherence ${s.coherence}. ${s.cycle_count || 0} cycles completed.`);
  }

  // Open obligations
  const open = (_loadedObligs || []).filter(o => o.status !== 'resolved');
  if (open.length) {
    chunks.push(`${open.length} open obligation${open.length !== 1 ? 's' : ''}.`);
    open.forEach(o => {
      chunks.push(`${o.id}. ${cleanText(o.statement)}`);
      const prog = (o.progress || '').split('|')[0].trim();
      if (prog) chunks.push(`Progress: ${cleanText(prog)}`);
    });
  }

  // Node compresses
  if (_loadedProjects.length) {
    chunks.push(`${_loadedProjects.length} knowledge node${_loadedProjects.length !== 1 ? 's' : ''}.`);
    _loadedProjects.forEach(n => {
      if (n.compress) chunks.push(`${cleanText(n.title)}: ${cleanText(n.compress)}`);
    });
  }

  // Top genome symbols (highest recurrence first, top 7)
  if (_loadedSymbols) {
    const entries = Object.entries(_loadedSymbols)
      .filter(([k]) => k !== '_meta')
      .sort((a, b) => (b[1].recurrence || 0) - (a[1].recurrence || 0))
      .slice(0, 7);
    if (entries.length) {
      chunks.push(`Genome registry. ${entries.length} confirmed symbols.`);
      entries.forEach(([key, sym]) => {
        const name = key.replace(/_/g, ' ');
        chunks.push(`${name}. ${cleanText(sym.genome_role || sym.canonical || '')}`);
      });
    }
  }

  return chunks;
}

// Generation counter — incremented on every start/stop.
// Each speakChunks callback carries the gen it was born into.
// If gen no longer matches _speakGen, the chain is stale and exits.
let _speakGen = 0;

function speakChunks(chunks, idx, gen) {
  // Stale chain from a previous run — bail out silently
  if (gen !== _speakGen) return;

  if (!_speaking || idx >= chunks.length) {
    stopSpeak();
    return;
  }
  const section = idx === 0 ? 'state' :
    idx <= (_loadedObligs || []).filter(o => o.status !== 'resolved').length * 2 ? 'obligations' :
    idx <= (_loadedProjects.length * 1 + 2) ? 'nodes' : 'symbols';
  document.getElementById('speak-status').textContent =
    `${idx + 1} / ${chunks.length}  ·  ${section}`;

  const u = new SpeechSynthesisUtterance(chunks[idx]);
  const voice = _getVoice();
  if (voice) u.voice = voice;
  u.rate  = _getRate();
  u.pitch = 1.0;
  u.onend   = () => speakChunks(chunks, idx + 1, gen);
  u.onerror = () => speakChunks(chunks, idx + 1, gen);
  window.speechSynthesis.speak(u);
}

// ── Voice selector ────────────────────────────────────────────────────────────

// Voices removed entirely (musical / gimmick)
const REMOVE_VOICES  = ['Bad News','Bells','Cellos','Good News','Organ','Bubbles','Jester'];
// Voices with dedicated one-click buttons (bypass dropdown)
const CHAR_VOICES    = ['Boing','Fred','Trinoids','Zarvox','Superstar'];
// Voices in the "Other" dropdown
const OTHER_VOICES   = ['Whisper','Ralph','Kathy','Junior','Wobble','Baah','Albert'];
// Default preference for main select
const PREFERRED_MAIN = ['Aaron','Alex','Samantha','Tom','Daniel'];

let _voices = [];
let _charVoiceOverride = null;   // set when a char button is clicked

function _loadVoices() {
  _voices = window.speechSynthesis.getVoices();
  if (!_voices.length) return;

  const isRemoved = name => REMOVE_VOICES.some(r => name.toLowerCase().includes(r.toLowerCase()));
  const isChar    = name => CHAR_VOICES.some(c => name.toLowerCase().includes(c.toLowerCase()));
  const isOther   = name => OTHER_VOICES.some(o => name.toLowerCase().includes(o.toLowerCase()));
  const isUSEng   = lang => lang === 'en-US' || lang === 'en_US';

  // Populate main select — US English, excluding removed/char voices (other voices included)
  const mainSel = document.getElementById('voice-select');
  mainSel.innerHTML = '';
  const mainVoices = _voices.filter(v =>
    isUSEng(v.lang) && !isRemoved(v.name) && !isChar(v.name)
  ).sort((a, b) => a.name.localeCompare(b.name));
  mainVoices.forEach(v => {
    const opt = document.createElement('option');
    opt.value = v.name; opt.textContent = v.name;
    mainSel.appendChild(opt);
  });

  // Populate Languages select — all non-US-English voices
  const langSel = document.getElementById('voice-lang');
  langSel.innerHTML = '<option value="">—</option>';
  const langVoices = _voices.filter(v => !isUSEng(v.lang) && !isRemoved(v.name))
    .sort((a, b) => a.lang.localeCompare(b.lang) || a.name.localeCompare(b.name));
  langVoices.forEach(v => {
    const opt = document.createElement('option');
    opt.value = v.name; opt.textContent = `${v.name} (${v.lang})`;
    langSel.appendChild(opt);
  });

  // Restore saved rate
  const savedRate = localStorage.getItem('freed_rate');
  if (savedRate) {
    document.getElementById('rate-slider').value = savedRate;
    document.getElementById('rate-val').textContent = parseFloat(savedRate).toFixed(2);
  }

  // Restore saved voice or default to Trinoids preference
  const saved = localStorage.getItem('freed_voice');
  if (saved) {
    // Try main select first
    if ([...mainSel.options].find(o => o.value === saved)) {
      mainSel.value = saved;
    }
    // Otherwise it might be a char/other voice — that's fine, _getVoice handles it
  } else {
    for (const pref of PREFERRED_MAIN) {
      const match = mainVoices.find(v => v.name.includes(pref));
      if (match) { mainSel.value = match.name; break; }
    }
  }
}

function saveVoicePref() {
  const sel  = document.getElementById('voice-select');
  const rate = document.getElementById('rate-slider');
  _charVoiceOverride = null;   // picking from dropdown cancels char override
  document.querySelectorAll('.char-btn').forEach(b => b.classList.remove('char-active'));
  if (sel.value) localStorage.setItem('freed_voice', sel.value);
  if (rate.value) localStorage.setItem('freed_rate', rate.value);
}

function syncVoiceFrom(selectId) {
  const src = document.getElementById(selectId);
  if (!src.value) return;
  _charVoiceOverride = null;
  document.querySelectorAll('.char-btn').forEach(b => b.classList.remove('char-active'));
  localStorage.setItem('freed_voice', src.value);
  src.value = '';   // reset dropdown back to placeholder
}

function _getVoice() {
  const name = _charVoiceOverride || localStorage.getItem('freed_voice')
               || document.getElementById('voice-select').value;
  return _voices.find(v => v.name === name) || null;
}

function _getRate() {
  return parseFloat(document.getElementById('rate-slider').value) || 1.10;
}

function speakWithVoice(voiceKey) {
  // voiceKey is the macOS system name (e.g. 'Fred', 'Trinoids')
  const voice = _voices.find(v => v.name === voiceKey)
             || _voices.find(v => v.name.toLowerCase().includes(voiceKey.toLowerCase()));
  if (!voice) return;
  stopSpeak();
  _charVoiceOverride = voice.name;
  localStorage.setItem('freed_voice', voice.name);
  document.querySelectorAll('.char-btn').forEach(b => {
    b.classList.toggle('char-active', b.dataset.voice === voiceKey);
  });
  toggleSpeak();
}

// Voices load asynchronously on some browsers
if (window.speechSynthesis) {
  _loadVoices();
  window.speechSynthesis.onvoiceschanged = _loadVoices;
}

// ── Audio unlock (Bluetooth routing) ─────────────────────────────────────────

function _unlockAudio() {
  // Play a silent buffer through Web Audio API from within the user gesture.
  // This claims the media audio route (Bluetooth speakers, headphones) so
  // that speechSynthesis follows it instead of the accessibility channel.
  try {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return Promise.resolve();
    const ctx = new Ctx();
    const buf = ctx.createBuffer(1, 1, 22050);
    const src = ctx.createBufferSource();
    src.buffer = buf;
    src.connect(ctx.destination);
    src.start(0);
    return ctx.resume ? ctx.resume() : Promise.resolve();
  } catch(e) { return Promise.resolve(); }
}

// ── Speak ─────────────────────────────────────────────────────────────────────

function startSpeak() {
  if (!window.speechSynthesis) {
    alert('Speech synthesis not available in this browser.');
    return;
  }
  // Cancel any in-flight speech and invalidate stale callbacks
  _speaking = false;
  _speakGen++;
  window.speechSynthesis.cancel();

  const thisGen = _speakGen;
  const chunks  = buildDigest();
  if (!chunks.length) {
    document.getElementById('speak-status').textContent = 'Nothing loaded yet.';
    return;
  }

  _speaking = true;
  const btn = document.getElementById('speak-btn');
  btn.textContent = '■ STOP';
  btn.classList.add('speaking');

  // Small delay after cancel() — mobile browsers need a tick to flush the queue
  _unlockAudio().then(() => setTimeout(() => speakChunks(chunks, 0, thisGen), 120));
}

function stopSpeak() {
  _speakGen++;          // invalidate any callbacks still in flight
  _speaking = false;
  window.speechSynthesis.cancel();
  const btn = document.getElementById('speak-btn');
  btn.textContent = '▶ SPEAK DIGEST';
  btn.classList.remove('speaking');
  document.getElementById('speak-status').textContent = '';
}

function toggleSpeak() {
  _speaking ? stopSpeak() : startSpeak();
}

// ── Render genome symbols ─────────────────────────────────────────────────────

function renderSymbols(data) {
  const el = document.getElementById('symbols');
  if (!data) { el.innerHTML = '<div class="loading">Unavailable.</div>'; return; }

  const meta = data._meta || {};
  const latestGen = meta.generation || 0;

  const entries = Object.entries(data)
    .filter(([k]) => k !== '_meta')
    .sort((a, b) => (b[1].recurrence || 0) - (a[1].recurrence || 0));

  if (!entries.length) { el.innerHTML = '<div class="loading">No symbols yet.</div>'; return; }

  const html = entries.map(([key, sym]) => {
    const rec = sym.recurrence || 0;
    const pct = Math.round(rec * 100);
    const isNew = sym.mining_generation && sym.mining_generation >= latestGen - 1;

    const confirmedHtml = (sym.confirmed_by || []).map(c => {
      const label = c.includes(':') ? c.split(':')[1].replace(/_/g, ' ').slice(0, 30) : c.replace(/_/g, ' ');
      return `<span class="symbol-confirmed-tag">${label}</span>`;
    }).join('');

    const driftHtml = (sym.known_drift || []).length
      ? `<div class="symbol-drift">
           <div class="symbol-drift-label">known drift</div>
           ${(sym.known_drift || []).map(d => `<div>· ${d}</div>`).join('')}
         </div>`
      : '';

    return `<div class="symbol">
  <div class="symbol-header">
    <span class="symbol-name">${key.replace(/_/g, '_')}</span>
    ${isNew ? '<span class="symbol-badge new-badge">new</span>' : ''}
    <span class="symbol-recurrence">
      <span class="symbol-bar-track"><span class="symbol-bar-fill" style="width:${pct}%"></span></span>
      ${rec.toFixed(2)}
      ${sym.mining_recurrence_count ? `· ${sym.mining_recurrence_count}× nodes` : ''}
    </span>
  </div>
  <div class="symbol-canonical">${sym.canonical || ''}</div>
  ${sym.genome_role ? `<div class="symbol-role">${sym.genome_role}</div>` : ''}
  ${confirmedHtml ? `<div class="symbol-confirmed">${confirmedHtml}</div>` : ''}
  ${driftHtml}
</div>`;
  }).join('');

  const countLine = `<div style="color:var(--muted);font-size:0.72rem;margin-bottom:1rem">${entries.length} symbols · gen ${latestGen} · sorted by recurrence</div>`;
  el.innerHTML = countLine + html;
}

// ── Init ──────────────────────────────────────────────────────────────────────

async function init() {
  const [state, obligations, cycles, symbols, projects] = await Promise.all([
    load('state.json'),
    load('obligations.json'),
    load('cycles.json'),
    load('symbols.json'),
    load('projects.json'),
  ]);
  _loadedState    = state;
  _loadedObligs   = obligations;
  _loadedProjects = projects || [];
  _loadedSymbols  = symbols;
  renderState(state);
  renderObligations(obligations);
  renderCycles(cycles);
  renderSymbols(symbols);
  // Count badges on right-column section summaries
  _setBadge('cycles',   (cycles||[]).length);
  _setBadge('symbols',  Object.keys(symbols||{}).length);
  _setBadge('projects', _loadedProjects.length);
}

init();

// Refresh every 5 minutes
setInterval(init, 5 * 60 * 1000);

// ── Daemon status polling (every 30s) ─────────────────────────────────────────
async function pollStatus() {
  try {
    const s = await load('status.json');
    if (!s) return;
    const phaseEl  = document.getElementById('daemon-phase');
    const detailEl = document.getElementById('daemon-detail');
    const phase    = (s.phase || 'IDLE').toLowerCase().replace(/[^a-z-]/g, '');
    phaseEl.textContent  = (s.phase || 'IDLE').toUpperCase();
    phaseEl.className    = `daemon-phase ${phase}`;
    detailEl.textContent = s.detail || '—';
    // Pulse: active (red) when working, steady green when idle
    const pulse = document.getElementById('pulse');
    if (pulse) pulse.style.borderColor = phase === 'idle' ? 'var(--green)' : 'var(--accent)';
    // Light up kernel chain step
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
</html>
"""
