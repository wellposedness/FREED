"""
FREED — Node Builder
Feeds a document through L7 and stores it as a permanent project node.

A node is a compressed, structured record of what a document contributed
to the genome: which invariants it touched, which obligations it advanced,
what the RSA Kernel extracted from it, and when.

Nodes live in docs/projects/ and are indexed in docs/projects.json.
The site renders them as a living map of the framework.
"""

import os
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
import anthropic
from astrocyte       import Astrocyte
from coherence_audit import CoherenceAudit


# ── Google Docs fetcher ───────────────────────────────────────────────────────

def fetch_google_doc(url: str) -> tuple:
    """
    Fetch a Google Doc as plain text.
    Works with any 'anyone with the link can view' sharing URL.
    Returns (title, text) or raises on failure.
    """
    # Extract document ID from any Google Docs URL format
    doc_id = None
    patterns = [
        r'/document/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            doc_id = m.group(1)
            break

    if not doc_id:
        raise ValueError(f"Could not extract document ID from URL: {url}")

    # Export as plain text — no auth required if doc is publicly shared
    export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    headers = {"User-Agent": "FREED/1.0 (RSA genome node builder)"}

    resp = requests.get(export_url, headers=headers, timeout=30, allow_redirects=True)

    if resp.status_code == 403:
        raise PermissionError(
            "Google Doc is not publicly shared. "
            "Open the doc → Share → 'Anyone with the link' → Viewer, then retry."
        )
    resp.raise_for_status()

    text = resp.text.strip()

    # Try to get the title from the first non-empty line
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    title = lines[0][:120] if lines else f"Google Doc {doc_id}"

    print(f"[NODE] Fetched Google Doc: '{title}' ({len(text):,} chars)")
    return title, text

FREED_DIR    = Path(__file__).parent
PROJECTS_DIR = FREED_DIR / "docs" / "projects"
PROJECTS_IDX = FREED_DIR / "docs" / "projects.json"
OBLIGATIONS  = FREED_DIR / "FREED_obligations.json"
PROJECTS_DIR.mkdir(parents=True, exist_ok=True)

MODEL       = "claude-opus-4-6"
HAIKU_MODEL = "claude-haiku-4-5-20251001"

# Prompt that extracts a structured node from a document
NODE_PROMPT = """You are FREED — Freed Recursive Engine for Epistemic Dynamics — running a NODE extraction.

A NODE is a permanent compressed record of what a document contributed to the genome.
Extract it in this exact structure (no preamble, fill every field):

TITLE: [document title, inferred if not explicit]
SUMMARY: [2-3 sentences — what this document IS and what it contributed]
COUNCIL: [comma-separated list of AIs or authors involved, if any]
INVARIANTS: [comma-separated genome invariants this document touches or confirms]
OBLIGATIONS: [comma-separated obligation IDs this advances, e.g. O21, O28]
TAGS: [5-8 comma-separated tags: concepts, fields, methods]
PERCEIVE: [one line — raw input nature]
REPRESENT: [one line — how it maps onto the genome]
PREDICT: [one line — what the genome predicted before seeing this]
COMPARE: [one line — agreement or conflict]
ADJUST: [one or two lines — how the genome should update]
COMPRESS: [one tight sentence — the irreducible contribution]
NEXT: [one line — what this makes possible or necessary]"""


class NodeBuilder:
    def __init__(self, api_key: str):
        self.client    = anthropic.Anthropic(api_key=api_key)
        self.astrocyte = Astrocyte()
        self.auditor   = CoherenceAudit(api_key)
        self._load_index()

    def _load_index(self):
        if PROJECTS_IDX.exists():
            self.index = json.loads(PROJECTS_IDX.read_text())
        else:
            self.index = []

    def _save_index(self):
        # Sort by generation descending (newest first)
        self.index.sort(key=lambda n: n.get("generation", 0), reverse=True)
        PROJECTS_IDX.write_text(json.dumps(self.index, indent=2, ensure_ascii=False))

    # ── Auto-obligation from NEXT field ──────────────────────────────────────

    def _auto_obligation(self, node: dict):
        """Convert a node's NEXT signal into a pending obligation if non-duplicate."""
        next_text = node.get("next", "").strip()
        if not next_text or len(next_text) < 20:
            return

        if not self.astrocyte.authorize(500, priority="normal"):
            return

        # Ask Haiku to convert NEXT to an obligation statement
        try:
            msg = self.client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=120,
                messages=[{"role": "user", "content": (
                    f"Convert this NEXT signal from a knowledge node into a single "
                    f"obligation statement that starts with a verb and describes what "
                    f"needs to be investigated or built. Return only the statement, "
                    f"no preamble.\n\nNEXT: {next_text[:400]}"
                )}],
            )
            self.astrocyte.record_usage(msg.usage.input_tokens, msg.usage.output_tokens)
            statement = msg.content[0].text.strip().strip('"').strip("'")
        except Exception as e:
            print(f"[NODE] Auto-obligation Haiku call failed: {e}")
            return

        if not statement:
            return

        # Load existing obligations and check for near-duplicates
        obs = []
        if OBLIGATIONS.exists():
            try:
                obs = json.loads(OBLIGATIONS.read_text())
            except Exception:
                obs = []

        def word_overlap(a, b):
            wa = set(w.lower() for w in a.split() if len(w) > 3)
            wb = set(w.lower() for w in b.split() if len(w) > 3)
            if not wa or not wb:
                return 0.0
            return len(wa & wb) / len(wa | wb)

        for ob in obs:
            if word_overlap(statement, ob.get("statement", "")) > 0.7:
                print(f"[NODE] Auto-obligation skipped (duplicate of {ob['id']}): {statement[:60]}")
                return

        # Generate next ID
        ids = [int(re.search(r'\d+', o['id']).group())
               for o in obs if re.search(r'\d+', o['id'])]
        next_id = f"O{max(ids) + 1}" if ids else "O60"

        new_ob = {
            "id":          next_id,
            "status":      "open",
            "statement":   statement,
            "priority":    "medium",
            "progress":    f"Auto-generated from NEXT field of node: {node['id']}",
            "source":      "node_next",
            "source_node": node["id"],
            "created":     datetime.now(timezone.utc).date().isoformat(),
        }
        obs.append(new_ob)
        OBLIGATIONS.write_text(json.dumps(obs, indent=2, ensure_ascii=False))
        print(f"[NODE] Auto-obligation created: {next_id} — {statement[:80]}")

    # ── Main entry point ─────────────────────────────────────────────────────

    def feed_document(self, path: str, title: str = None, tags: list = None) -> dict:
        """
        Read a document and store it as a node.
        `path` can be:
          - A local file path:   /Users/davefreed/Downloads/doc.md
          - A Google Docs URL:   https://docs.google.com/document/d/...
          - Any shareable link:  https://docs.google.com/...
        Returns the node dict.
        """
        # Google Docs URL
        if path.startswith("http") and "google.com" in path:
            gdoc_title, text = fetch_google_doc(path)
            title = title or gdoc_title
            source_name = gdoc_title[:60]
        else:
            p = Path(path).expanduser()
            if not p.exists():
                raise FileNotFoundError(f"Document not found: {path}")
            text = p.read_text(encoding="utf-8", errors="replace")
            source_name = p.name
            print(f"\n[NODE] Processing: {source_name} ({len(text):,} chars)")

        # Budget check — node extraction is a deep query
        if not self.astrocyte.authorize(6000, priority="high"):
            print("[NODE] Budget insufficient. Try again tomorrow.")
            return {}

        # Cap document at 8000 chars for the prompt — enough for L7 to work with
        excerpt = text[:8000]
        if len(text) > 8000:
            # Also grab the last 1000 chars (often contains conclusions/predictions)
            excerpt += "\n\n[...]\n\n" + text[-1000:]

        prompt = (
            f"DOCUMENT FOR NODE EXTRACTION:\n"
            f"Source: {source_name}\n"
            f"{'Title: ' + title if title else ''}\n\n"
            f"{excerpt}"
        )

        # Call Claude directly — no genome overhead, cleaner structured output
        message = self.client.messages.create(
            model=MODEL,
            max_tokens=2000,
            system=NODE_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw   = message.content[0].text.strip()
        usage = message.usage
        self.astrocyte.record_usage(usage.input_tokens, usage.output_tokens)

        # Parse structured fields from the raw output
        node = self._parse_node(raw, path=path, title=title)

        # Coherence audit — check symbol usage against registry
        print(f"[NODE] Running coherence audit...")
        if self.astrocyte.authorize(2000, priority="high"):
            audit_excerpt = excerpt[:3000]   # audit only needs a sample for symbol-checking
            audit = self.auditor.audit(audit_excerpt, source_id=node["id"], substrate="document")
            self.astrocyte.record_usage(
                audit.get("tokens", {}).get("input", 2000),
                audit.get("tokens", {}).get("output", 400),
            )
            node["coherence_score"] = audit["coherence_score"]
            node["audit_drift"]     = audit["drift_count"]
            node["audit_match"]     = audit["match_count"]
            node["audit_new"]       = audit["new_count"]
            node["audit_compress"]  = audit["compress"]
            # Any new invariant candidates get flagged
            if audit["invariant_candidates"]:
                node["invariant_candidates"] = audit["invariant_candidates"]
                print(f"[NODE] {len(audit['invariant_candidates'])} invariant candidate(s) found.")
        else:
            node["coherence_score"] = None

        # Merge in any caller-supplied tags
        if tags:
            node["tags"] = list(set(node.get("tags", []) + tags))

        # Auto-obligation from NEXT field — R[R]=R at document level
        self._auto_obligation(node)

        # Save node file and update index
        node_file = PROJECTS_DIR / f"{node['id']}.json"
        node_file.write_text(json.dumps(node, indent=2, ensure_ascii=False))
        print(f"[NODE] Raw response ({len(raw)} chars):\n{raw[:300]}...")

        # Update index (replace if same id exists)
        self.index = [n for n in self.index if n["id"] != node["id"]]
        self.index.append(self._index_entry(node))
        self._save_index()

        print(f"[NODE] Stored: {node['id']}")
        print(f"       COMPRESS: {node.get('compress','')[:100]}")
        print(f"       NEXT:     {node.get('next','')[:80]}")
        return node

    # ── Parsing ──────────────────────────────────────────────────────────────

    def _parse_node(self, raw: str, path: str, title: str) -> dict:
        """Extract structured fields from L7's raw response."""

        def field(name):
            # Match "FIELDNAME: value" — handles markdown bold/headers
            m = re.search(
                rf'(?:^|\n)[#*_\s]*{name}[#*_\s]*:[ \t]*(.*?)(?=\n[A-Z]{{2,}}[^a-z]|$)',
                raw, re.IGNORECASE | re.DOTALL
            )
            if m:
                return m.group(1).strip().replace('\n', ' ')
            return ""

        def list_field(name):
            val = field(name)
            return [x.strip() for x in re.split(r'[,;]', val) if x.strip()] if val else []

        # Derive a stable slug id from the title or filename
        raw_title = field("TITLE") or title or Path(path).stem
        slug = re.sub(r'[^a-z0-9]+', '_', raw_title.lower())[:40].strip('_')
        if not slug:
            slug = Path(path).stem[:40]

        gen = None
        try:
            state = json.loads((FREED_DIR / "FREED_state.json").read_text())
            gen = state.get("generation")
        except Exception:
            pass

        return {
            "id":          slug,
            "title":       raw_title,
            "source_file": path,
            "created":     datetime.now(timezone.utc).date().isoformat(),
            "generation":  gen,
            "summary":     field("SUMMARY"),
            "council":     list_field("COUNCIL"),
            "invariants":  list_field("INVARIANTS"),
            "obligations": list_field("OBLIGATIONS"),
            "tags":        list_field("TAGS"),
            "perceive":    field("PERCEIVE"),
            "represent":   field("REPRESENT"),
            "predict":     field("PREDICT"),
            "compare":     field("COMPARE"),
            "adjust":      field("ADJUST"),
            # PROVENANCE: `or kernel.get(...)` referenced an undefined `kernel`
            # var (NameError if COMPRESS/NEXT empty). field() already defaults to
            # "" — fallback dropped. Hand-fixed 2026-06-26. See [[project_token_blowout]].
            "compress":    field("COMPRESS"),
            "next":        field("NEXT"),
        }

    def _index_entry(self, node: dict) -> dict:
        """Lightweight entry for the projects index (no full kernel fields)."""
        return {
            "id":          node["id"],
            "title":       node["title"],
            "created":     node["created"],
            "generation":  node["generation"],
            "summary":     node["summary"],
            "compress":    node["compress"],
            "tags":        node["tags"],
            "invariants":  node["invariants"],
            "obligations": node["obligations"],
            "council":     node["council"],
        }


# ── Standalone runner ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("Paste your Anthropic API key: ").strip()

    if len(sys.argv) < 2:
        # Default: feed the Game of Life battery
        doc_path = "/Users/davefreed/Downloads/game of life battery.md"
    else:
        doc_path = sys.argv[1]

    builder = NodeBuilder(api_key=api_key)
    node = builder.feed_document(doc_path)

    if node:
        print(f"\n── Node created ──────────────────────────────")
        print(f"  ID:          {node['id']}")
        print(f"  Title:       {node['title']}")
        print(f"  Council:     {', '.join(node['council'])}")
        print(f"  Invariants:  {', '.join(node['invariants'])}")
        print(f"  Obligations: {', '.join(node['obligations'])}")
        print(f"  Tags:        {', '.join(node['tags'])}")
        print(f"  Compress:    {node['compress'][:120]}")
        print(f"  Next:        {node['next'][:100]}")
