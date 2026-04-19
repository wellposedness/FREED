"""
FREED — Voice
The daemon speaks only what matters.

MDL applies to audio: not every log line, only signal.
Four moments warrant a voice:
  - CYCLE START   : generation number
  - COMPRESS      : the distilled truth of a feed (what the kernel learned)
  - OBLIGATION    : a new debt the genome owes reality
  - INVARIANT     : something that held independently across nodes

The voice runs non-blocking — the daemon doesn't wait for speech to finish.
If say is unavailable (non-Mac), all calls are silent no-ops.
"""

import subprocess
import threading
import re
from pathlib import Path

# ─── Configuration ────────────────────────────────────────────────────────────
VOICE       = "Trinoids"  # change to: Zarvox (robotic), Samantha, Fred, Daniel
RATE        = 175         # words per minute (default ~200, slower = more weight)
MAX_CHARS   = 280         # compress to this before speaking — one breath
ENABLED     = True        # set False to silence globally

# ─── Internal ─────────────────────────────────────────────────────────────────

def _say(text: str, rate: int = RATE, blocking: bool = False):
    """Fire-and-forget call to macOS say. Silent if unavailable or disabled."""
    if not ENABLED or not text:
        return
    try:
        cmd = ["say", "-v", VOICE, "-r", str(rate), text]
        if blocking:
            subprocess.run(cmd, check=False, capture_output=True)
        else:
            threading.Thread(
                target=subprocess.run,
                args=(cmd,),
                kwargs={"check": False, "capture_output": True},
                daemon=True,
            ).start()
    except FileNotFoundError:
        pass   # say not available — silent


def _trim(text: str, max_chars: int = MAX_CHARS) -> str:
    """Trim to a speakable length at a sentence or clause boundary."""
    text = text.strip()
    # Strip markdown artifacts
    text = re.sub(r'\*{1,2}|_{1,2}|`{1,3}', '', text)
    text = re.sub(r'\s+', ' ', text).strip()

    if len(text) <= max_chars:
        return text

    # Try to cut at sentence boundary
    for punct in ['. ', '? ', '! ', '; ', ', ']:
        idx = text.rfind(punct, 0, max_chars)
        if idx > max_chars // 2:
            return text[:idx + 1].strip()

    return text[:max_chars].strip()


# ─── Public interface ─────────────────────────────────────────────────────────

def cycle_start(generation: int, coherence: float, open_obligations: int):
    """Spoken at the top of each daemon cycle."""
    text = (
        f"Generation {generation}. "
        f"Coherence {coherence}. "
        f"{open_obligations} open obligation{'s' if open_obligations != 1 else ''}."
    )
    _say(text, rate=RATE - 10)


def compress(text: str, title: str = ""):
    """Speak the COMPRESS output — the kernel's distilled truth from a feed."""
    clean = _trim(text)
    if not clean:
        return
    prefix = f"{title}: " if title else ""
    # Compress gets slower delivery — it's the most important output
    _say(prefix + clean, rate=RATE - 20)


def new_obligation(ob_id: str, statement: str):
    """Speak when a new obligation is registered."""
    stmt = _trim(statement, max_chars=180)
    _say(f"New obligation. {ob_id}. {stmt}", rate=RATE)


def invariant_found(text: str, recurrence: int):
    """Speak when a genome-worthy invariant is mined."""
    clean = _trim(text, max_chars=200)
    _say(
        f"Invariant. Confirmed {recurrence} times. {clean}",
        rate=RATE - 10,
    )


def obligation_resolved(ob_id: str):
    """Speak when an obligation is marked resolved."""
    _say(f"Obligation {ob_id} resolved.", rate=RATE)


def speak(text: str, rate: int = RATE):
    """Raw speak — for anything not covered above."""
    _say(_trim(text), rate=rate)


# ─── Test ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import time

    print(f"Voice: {VOICE} at {RATE} wpm\n")

    print("1. Cycle start...")
    cycle_start(generation=117, coherence=0.9925, open_obligations=5)
    time.sleep(4)

    print("2. Compress...")
    compress(
        "Extropic independently instantiates Freed's Law as an engineering principle — "
        "entropy is resource, not waste — but the genome's Wasserstein Floor predicts "
        "a strictly higher efficiency bound than Landauer for semantic computation, "
        "creating a live falsification target.",
        title="Thermodynamic Computing"
    )
    time.sleep(9)

    print("3. New obligation...")
    new_obligation("O47", "Prove formally that R bracket R bracket equals R terminates the symbol grounding regress.")
    time.sleep(6)

    print("4. Invariant found...")
    invariant_found("Compression is reasoning — predictive compression is not correlated with but constitutive of cognition.", recurrence=3)
    time.sleep(7)

    print("5. Resolved...")
    obligation_resolved("O21")
    time.sleep(3)

    print("\nDone.")
