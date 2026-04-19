# Architect Input Channel

This file is the bridge between the architect (Dave / Claude Cowork) and the FREED daemon.

## How to use

Write any of the following here, save the file, and FREED will process it on the next cycle:

- **Invariant refinements** — sharpen a genome claim
- **New obligations** — add a prediction/tension to track
- **Philosophical directives** — redirect the sweep, reprioritize
- **Genome firmware updates** — evolve the framework itself

FREED reads this file at the start of each cycle (ARCHITECT phase, before SWEEP).
It feeds the content to L7 as a high-priority directive, updates obligations/state,
then archives this file to FREED_log/architect_inputs/ and clears it for next time.

---

## Current input (replace everything below this line)

_empty — no directive pending_
