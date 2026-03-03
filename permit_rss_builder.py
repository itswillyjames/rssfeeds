#!/usr/bin/env python3
"""Minimal, stable permit feed builder.

Fetches recent permits from configured Socrata endpoints (Chicago & San Francisco),
builds an RSS feed via feedgen, and writes a JSON mirror. All datetimes use
datetime.now(timezone.utc) to ensure timezone-aware timestamps for CI (GitHub
Actions) compatibility.
"""
from datetime import datetime, timedelta, timezone
import json
import os
import sys
from typing import Any, Dict, List

import requests
from feedgen.feed import FeedGenerator


DEFAULT_OUTPUT_XML = "master_permit_feed.xml"
DEFAULT_OUTPUT_JSON = "master_permit_feed.json"

# Default Socrata endpoints and date fields. These can be overridden via env vars.
SOURCES = [
    {
        "name": "Chicago",
        "url": os.environ.get(
            "CHICAGO_PERMIT_URL", "https://data.cityofchicago.org/resource/ydr8-5enu.json"
        ),
        "date_field": os.environ.get("CHICAGO_DATE_FIELD", "issued_date"),
    },
    {
        "name": "San Francisco",
        "url": os.environ.get(
            "SF_PERMIT_URL", "https://data.sfgov.org/resource/6a7x-dm8c.json"
        ),
        "date_field": os.environ.get("SF_DATE_FIELD", "issued_date"),
    },
]


def iso_since(days: int = 7) -> str:
    """Return an ISO8601 timestamp `days` ago (timezone-aware)."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    return since.isoformat()


def fetch_permits(source: Dict[str, str], since_iso: str, limit: int = 10000) -> List[Dict[str, Any]]:
    """Fetch permits from a Socrata-like endpoint.

    The function is defensive: network errors or bad responses return an empty list.
    """
    url = source.get("url")
    date_field = source.get("date_field")
    params = {
        "$where": f"{date_field} >= '{since_iso}'",
        "$limit": limit,
        "$order": f"{date_field} DESC",
    }
    headers = {"Accept": "application/json"}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        # If the API wraps results, try to extract a list safely.
        if isinstance(data, dict):
            # common Socrata responses are lists; fall back to values
            for v in data.values():
                if isinstance(v, list):
                    return v
        print(f"[warn] Unexpected response format from {source.get('name')}", file=sys.stderr)
        return []
    except requests.RequestException as exc:
        print(f"[error] Failed to fetch {source.get('name')}: {exc}", file=sys.stderr)
        return []
    except ValueError as exc:
        print(f"[error] Invalid JSON from {source.get('name')}: {exc}", file=sys.stderr)
        return []


def build_feed(all_records: List[Dict[str, Any]]) -> FeedGenerator:
    now = datetime.now(timezone.utc)
    fg = FeedGenerator()
    fg.id("urn:uuid:master-permit-feed")
    fg.title("Master Permit Feed")
    fg.link(href="https://example.local/permits", rel="alternate")
    fg.description("Aggregated recent permits from multiple city open-data portals.")
    fg.language("en")
    fg.updated(now)

    for rec in all_records:
        try:
            entry = fg.add_entry()
            # Use stable, deterministic id if available, else fallback to an index-like id
            rec_id = rec.get("id") or rec.get("permit_number") or rec.get("permit") or rec.get("case_number")
            if not rec_id:
                rec_id = json.dumps(rec, sort_keys=True)[:200]
            entry.id(str(rec_id))
            title = rec.get("permit_type") or rec.get("description") or rec.get("type") or "Permit"
            entry.title(str(title))
            # Link if present, otherwise omit
            link = rec.get("permit_url") or rec.get("url")
            if link:
                entry.link(href=link)
            # Description: include a compact JSON snippet to keep feed helpful
            desc = rec.get("address") or rec.get("location") or rec.get("description")
            if not desc:
                # keep it short and safe
                desc = json.dumps({k: v for k, v in rec.items() if k in ("address", "description")}, default=str)
            entry.description(str(desc))
            # Use timezone-aware now for published/updated to satisfy CI environments
            entry.published(now)
            entry.updated(now)
        except Exception as exc:
            print(f"[warn] Skipping record due to error: {exc}", file=sys.stderr)

    return fg


def write_outputs(fg: FeedGenerator, records: List[Dict[str, Any]], xml_path: str, json_path: str) -> None:
    try:
        fg.rss_file(xml_path)
    except Exception as exc:
        print(f"[error] Failed to write XML feed: {exc}", file=sys.stderr)

    try:
        # Create a compact JSON feed with some fields for each record.
        minimal = []
        for r in records:
            minimal.append({
                "id": r.get("id") or r.get("permit_number") or r.get("case_number"),
                "source": r.get("agency") or r.get("source") or None,
                "summary": r.get("description") or r.get("permit_type") or r.get("address"),
                "raw": r,
            })
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "items": minimal}, fh, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[error] Failed to write JSON feed: {exc}", file=sys.stderr)


def main() -> None:
    since_iso = iso_since(days=7)
    all_records: List[Dict[str, Any]] = []

    for src in SOURCES:
        recs = fetch_permits(src, since_iso)
        if recs:
            # tag records with source for easier debugging downstream
            for r in recs:
                if "source" not in r:
                    r["source"] = src.get("name")
            all_records.extend(recs)

    # Keep order stable: newest first by insertion order from sources.
    fg = build_feed(all_records)

    out_xml = os.environ.get("OUT_XML", DEFAULT_OUTPUT_XML)
    out_json = os.environ.get("OUT_JSON", DEFAULT_OUTPUT_JSON)
    write_outputs(fg, all_records, out_xml, out_json)


if __name__ == "__main__":
    main()
