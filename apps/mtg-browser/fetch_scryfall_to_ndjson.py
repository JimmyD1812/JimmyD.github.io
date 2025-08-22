#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import sys
from typing import Dict, Any, Optional, List, TextIO
from decimal import Decimal

import requests
import ijson  # pip install ijson requests

BULK_INDEX_URL = "https://api.scryfall.com/bulk-data"
UA = "mtg-collection-updater/1.0 (+https://github.com)"


def get_bulk_meta(dataset_type: str, timeout: float = 60.0) -> Dict[str, Any]:
    """
    Fetch bulk-data index and return the entry for the requested dataset_type,
    e.g. "oracle_cards" or "default_cards".
    """
    r = requests.get(BULK_INDEX_URL, headers={"User-Agent": UA}, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    if payload.get("object") != "list" or "data" not in payload:
        raise RuntimeError("Unexpected bulk-data payload shape")
    for entry in payload["data"]:
        if isinstance(entry, dict) and entry.get("type") == dataset_type:
            if not entry.get("download_uri"):
                raise RuntimeError(f"{dataset_type} entry missing download_uri")
            return entry
    raise RuntimeError(f"No '{dataset_type}' entry found in bulk-data index")


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _open_writer(path: str) -> TextIO:
    if path.lower().endswith(".gz"):
        return gzip.open(path, "wt", encoding="utf-8")
    return open(path, "w", encoding="utf-8")


def stream_bulk_json_array_to_ndjson(
    download_uri: str,
    out_paths: List[str],
    timeout: float = 600.0,
    limit: Optional[int] = None,
) -> int:
    """
    Stream the big JSON array at download_uri into one or more NDJSON files.
    Each object is written as a single line to every path in out_paths.
    Returns lines written.
    """
    if not out_paths:
        raise ValueError("out_paths must contain at least one path")

    with requests.get(
        download_uri,
        headers={"User-Agent": UA},
        stream=True,
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = True  # transparent gzip decode if server uses it

        writers = [_open_writer(p) for p in out_paths]
        count = 0
        try:
            for obj in ijson.items(resp.raw, "item"):
                line = json.dumps(obj, ensure_ascii=False, default=_json_default) + "\n"
                for w in writers:
                    w.write(line)
                count += 1
                if limit and count >= limit:
                    break
        finally:
            for w in writers:
                w.close()
        return count


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Fetch Scryfall oracle_cards and output NDJSON/NDJSON.GZ.")
    p.add_argument("-o", "--output", default="cards.ndjson.gz", help="Primary output path (default: cards.ndjson.gz)")
    p.add_argument("--also-plain", default=None, help="Optional: also write a plain NDJSON to this path (e.g., cards.ndjson)")
    p.add_argument("--timeout", type=float, default=600.0, help="HTTP timeout seconds (default: 600)")
    p.add_argument("--limit", type=int, default=None, help="TESTING: only write first N cards")
    args = p.parse_args(argv)

    # Switch to Oracle Cards dataset
    meta = get_bulk_meta("oracle_cards", timeout=min(60.0, args.timeout))
    print(
        f"[info] oracle_cards: updated_at={meta.get('updated_at')} size={meta.get('size')} enc={meta.get('content_encoding')}",
        file=sys.stderr,
    )
    print(f"[info] downloading: {meta['download_uri']}", file=sys.stderr)

    out_paths = [args.output]
    if args.also_plain:
        out_paths.append(args.also_plain)

    written = stream_bulk_json_array_to_ndjson(
        meta["download_uri"],
        out_paths,
        timeout=args.timeout,
        limit=args.limit,
    )
    print(f"[done] wrote {written} NDJSON lines -> {', '.join(out_paths)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
