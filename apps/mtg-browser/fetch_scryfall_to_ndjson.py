#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import sys
from typing import Dict, Any, Optional
from decimal import Decimal  # <<< NEW

import requests
import ijson  # make sure requirements are installed

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


def _json_default(o):  # <<< NEW: teach json to handle Decimal
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def stream_default_cards_to_ndjson(
    download_uri: str,
    out_path: str,
    timeout: float = 600.0,
    limit: Optional[int] = None,
) -> int:
    with requests.get(
        download_uri,
        headers={"User-Agent": UA},
        stream=True,
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = True  # handle gzip from server if present

        out_ctx = gzip.open(out_path, "wt", encoding="utf-8") if out_path.lower().endswith(".gz") \
                  else open(out_path, "w", encoding="utf-8")

        count = 0
        with out_ctx as out_fp:
            for obj in ijson.items(resp.raw, "item"):  # stream each card
                out_fp.write(json.dumps(obj, ensure_ascii=False, default=_json_default))  # <<< default=...
                out_fp.write("\n")
                count += 1
                if limit and count >= limit:
                    break
        return count


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fetch Scryfall default_cards and output NDJSON/NDJSON.GZ.")
    p.add_argument("-o", "--output", default="cards.ndjson.gz", help="Output path (default: cards.ndjson.gz)")
    p.add_argument("--timeout", type=float, default=600.0, help="HTTP timeout seconds (default: 600)")
    p.add_argument("--limit", type=int, default=None, help="TESTING: only write first N cards")
    args = p.parse_args(argv)

    meta = get_default_cards_meta(timeout=min(60.0, args.timeout))
    print(
        f"[info] default_cards: updated_at={meta.get('updated_at')} size={meta.get('size')} enc={meta.get('content_encoding')}",
        file=sys.stderr,
    )
    print(f"[info] downloading: {meta['download_uri']}", file=sys.stderr)

    written = stream_default_cards_to_ndjson(
        meta["download_uri"], args.output, timeout=args.timeout, limit=args.limit
    )
    print(f"[done] wrote {written} NDJSON lines -> {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
