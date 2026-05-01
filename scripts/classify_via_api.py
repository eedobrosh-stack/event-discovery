"""
Classify artist batches via the Gemini API instead of the chat UI.

Reads batches from scripts/artist_batches/batch_NN.txt, sends each through
Gemini with structured-output enforcement, and writes the response to
scripts/gemini_responses/batch_NN.json — exactly the shape that
ingest_gemini_classifications.py already consumes.

Why structured output: the chat UI keeps truncating the trailing `}` once we
push past ~75 KB of output. The API's response_schema mode guarantees a valid
JSON object, so no more "echo '}' >> file" recovery dance.

Usage:
    # set GEMINI_API_KEY in /Users/eedo.b/supercaly/.env first
    python3 scripts/classify_via_api.py --batches 7-31
    python3 scripts/classify_via_api.py --batches 7,9,15
    python3 scripts/classify_via_api.py --batches 7        # single
    python3 scripts/classify_via_api.py --batches 7-10 --concurrency 3 --ingest

Default model: gemini-2.5-flash (free tier: 15 RPM / 1500 RPD).
Override with --model.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# Load .env without requiring python-dotenv (it isn't always installed in the
# user's site-packages). The file format we care about is "KEY=value".
ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
if ENV_PATH.is_file():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

try:
    from google import genai
    from google.genai import types as gtypes
except ImportError:
    sys.exit("google-genai not installed. Run: pip3 install --user google-genai")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("classify_via_api")

SCRIPTS = ROOT / "scripts"
BATCHES_DIR = SCRIPTS / "artist_batches"
RESP_DIR = SCRIPTS / "gemini_responses"
PROMPT_PATH = SCRIPTS / "gemini_followup_prompt.txt"

# Schema mirrors what the ingester expects. Strings only — confidence is open
# (we let the ingester reject malformed values rather than enforce in-schema,
# because Gemini sometimes coerces unexpected enum values to nulls).
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "classifications": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "artist": {"type": "string"},
                    "primary": {"type": "string"},
                    "secondary_1": {"type": "string", "nullable": True},
                    "secondary_2": {"type": "string", "nullable": True},
                    "confidence": {"type": "string"},
                },
                "required": ["artist", "primary", "confidence"],
                "propertyOrdering": [
                    "artist", "primary", "secondary_1", "secondary_2", "confidence",
                ],
            },
        },
    },
    "required": ["classifications"],
}


def parse_batch_spec(spec: str) -> list[int]:
    """Parse '7-10' / '7,9,15' / '7' / '7-10,15,20-22' into a sorted unique list."""
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return sorted(out)


def build_prompt(prompt_template: str, artists_text: str) -> str:
    """The follow-up prompt has a literal '[paste contents of …]' placeholder line.
    Replace that whole line with the artists block. If the placeholder isn't
    present (older prompt versions), just append."""
    placeholder_re = re.compile(r"^\[paste contents of [^\]]+\].*$", re.MULTILINE)
    if placeholder_re.search(prompt_template):
        return placeholder_re.sub(artists_text.rstrip(), prompt_template)
    return prompt_template.rstrip() + "\n\n" + artists_text.rstrip() + "\n"


def classify_one_batch(
    client: genai.Client,
    batch_num: int,
    model: str,
    prompt_template: str,
    overwrite: bool,
) -> tuple[int, str, dict | None]:
    """Run a single batch. Returns (batch_num, status, parsed_or_None).
    status ∈ {'ok', 'skipped', 'error:<msg>'}."""
    in_path = BATCHES_DIR / f"batch_{batch_num:02d}.txt"
    out_path = RESP_DIR / f"batch_{batch_num:02d}.json"

    if not in_path.is_file():
        return batch_num, f"error:input not found: {in_path}", None
    if out_path.is_file() and not overwrite:
        return batch_num, "skipped", None

    artists_text = in_path.read_text(encoding="utf-8")
    artist_count = sum(1 for line in artists_text.splitlines() if line.strip())
    full_prompt = build_prompt(prompt_template, artists_text)

    t0 = time.time()
    try:
        resp = client.models.generate_content(
            model=model,
            contents=full_prompt,
            config=gtypes.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=RESPONSE_SCHEMA,
                # 0 → deterministic. Classification isn't a creative task.
                temperature=0,
                # Generous output cap — 500 entries × ~150 chars × 1 token ≈ 25k tokens
                # but keep headroom for verbose entries.
                max_output_tokens=60000,
            ),
        )
    except Exception as e:
        return batch_num, f"error:api call failed: {e}", None

    elapsed = time.time() - t0
    raw = resp.text or ""
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        # Save the raw payload so we can inspect what went wrong.
        debug_path = out_path.with_suffix(".raw.txt")
        debug_path.write_text(raw, encoding="utf-8")
        return batch_num, f"error:json parse failed ({e}); raw → {debug_path.name}", None

    n_returned = len(parsed.get("classifications", []))
    out_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(
        f"  batch_{batch_num:02d}: {n_returned}/{artist_count} classifications "
        f"({elapsed:.1f}s) → {out_path.name}"
    )
    return batch_num, "ok", parsed


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--batches", required=True,
                    help="Batch spec: '7', '7-10', '7,9,15', '7-10,15'")
    ap.add_argument("--model", default="gemini-2.5-flash",
                    help="Gemini model id (default: gemini-2.5-flash)")
    ap.add_argument("--concurrency", type=int, default=5,
                    help="Parallel batches (default: 5; free tier is 15 RPM)")
    ap.add_argument("--overwrite", action="store_true",
                    help="Re-run even if batch_NN.json already exists")
    ap.add_argument("--ingest", action="store_true",
                    help="After classification, run ingest_gemini_classifications.py")
    args = ap.parse_args()

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        sys.exit("GEMINI_API_KEY not set in env or /Users/eedo.b/supercaly/.env")

    if not PROMPT_PATH.is_file():
        sys.exit(f"prompt template missing: {PROMPT_PATH}")
    prompt_template = PROMPT_PATH.read_text(encoding="utf-8")

    batch_nums = parse_batch_spec(args.batches)
    if not batch_nums:
        sys.exit("No batches selected")

    RESP_DIR.mkdir(parents=True, exist_ok=True)
    client = genai.Client(api_key=api_key)

    log.info(
        f"classifying {len(batch_nums)} batch(es): {batch_nums} | "
        f"model={args.model} | concurrency={args.concurrency}"
    )

    results: dict[int, str] = {}
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {
            pool.submit(
                classify_one_batch,
                client, n, args.model, prompt_template, args.overwrite,
            ): n
            for n in batch_nums
        }
        for fut in cf.as_completed(futures):
            n, status, _ = fut.result()
            results[n] = status
            if status.startswith("error"):
                log.error(f"  batch_{n:02d}: {status}")
            elif status == "skipped":
                log.info(f"  batch_{n:02d}: already exists, skipped (use --overwrite)")

    elapsed = time.time() - t0
    ok = sum(1 for s in results.values() if s == "ok")
    skipped = sum(1 for s in results.values() if s == "skipped")
    errors = sum(1 for s in results.values() if s.startswith("error"))
    log.info(
        f"\nDONE: {ok} ok, {skipped} skipped, {errors} errors "
        f"in {elapsed:.1f}s"
    )

    if args.ingest and ok:
        ok_files = [f"batch_{n:02d}.json" for n, s in sorted(results.items()) if s == "ok"]
        log.info(f"\nrunning ingester on {len(ok_files)} file(s)…")
        cmd = [
            sys.executable, str(SCRIPTS / "ingest_gemini_classifications.py"),
            "--batches", *ok_files,
        ]
        subprocess.run(cmd, cwd=ROOT, check=False)

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
