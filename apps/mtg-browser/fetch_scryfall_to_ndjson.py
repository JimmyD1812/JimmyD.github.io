#!/usr/bin/env python3
"""
Fetch Scryfall bulk 'default_cards' and convert to NDJSON.

- Queries https://api.scryfall.com/bulk-data
- Locates entry with type == 'default_cards'
- Streams the remote JSON array directly into NDJSON (one JSON object per line)
- Writes to --output (defaults to cards.ndjson.gz). If the output path ends with
  .gz, the file is written as gzip text; otherwise plain text.

Requires:
    pip install requests ijson

Usage:
    python fetch_scryfall_to_ndjson.py            # writes cards.ndjson.gz
    python fetch_scryfall_to_ndjson.py -o cards.ndjson   # plain text NDJSON
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from typing import Dict, Any, Optional

import requests

try:
    import ijson  # type: ignore
except Exception as e:
    print(
        "ERROR: This script requires `ijson` for streaming. Install with:\n"
        "    pip install ijson requests\n", file=sys.stderr
    )
    raise

BULK_INDEX_URL = "https://api.scryfall.com/bulk-data"
UA = "mtg-collection-updater/1.0 (+https://github.com)"


def get_default_cards_meta(timeout: float = 60.0) -> Dict[str, Any]:
    r = requests.get(BULK_INDEX_URL, headers={"User-Agent": UA}, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    if payload.get("object") != "list" or "data" not in payload:
        raise RuntimeError("Unexpected bulk-data payload shape")
    for entry in payload["data"]:
        if isinstance(entry, dict) and entry.get("type") == "default_cards":
            if not entry.get("download_uri"):
                raise RuntimeError("default_cards entry missing download_uri")
            return entry
    raise RuntimeError("No 'default_cards' entry found in bulk-data index")


def stream_default_cards_to_ndjson(download_uri: str, out_path: str, timeout: float = 600.0) -> int:
    """
    Stream the remote JSON array at `download_uri` into NDJSON at `out_path`.
    If out_path ends with '.gz', write gzip text. Returns number of lines written.
    """
    with requests.get(
        download_uri,
        headers={"User-Agent": UA},
        stream=True,
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = True  # transparently handle gzip from server

        # Choose writer based on extension
        if out_path.lower().endswith(".gz"):
            out_fp_ctx = gzip.open(out_path, "wt", encoding="utf-8")
        else:
            out_fp_ctx = open(out_path, "w", encoding="utf-8")

        count = 0
        with out_fp_ctx as out_fp:
            # The bulk file is a single JSON array; 'item' iterates array elements
            for obj in ijson.items(resp.raw, "item"):
                out_fp.write(json.dumps(obj, ensure_ascii=False))
                out_fp.write("\n")
                count += 1
        return count


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fetch Scryfall default_cards and output NDJSON/NDJSON.GZ.")
    p.add_argument("-o", "--output", default="cards.ndjson.gz", help="Output path (default: cards.ndjson.gz)")
    p.add_argument("--timeout", type=float, default=600.0, help="Download timeout seconds (default: 600)")
    args = p.parse_args(argv)

    meta = get_default_cards_meta(timeout=min(60.0, args.timeout))
    download_uri = meta["download_uri"]
    updated_at = meta.get("updated_at")
    size = meta.get("size")
    enc = meta.get("content_encoding")
    print(f"[info] default_cards: updated_at={updated_at}, size={size}, encoding={enc}", file=sys.stderr)
    print(f"[info] downloading: {download_uri}", file=sys.stderr)

    written = stream_default_cards_to_ndjson(download_uri, args.output, timeout=args.timeout)
    print(f"[done] wrote {written} NDJSON lines -> {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
