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
        # Verified field name from dataset metadata: ISSUE_DATE -> fieldName: issue_date
        "date_field": os.environ.get("CHICAGO_DATE_FIELD", "issue_date"),
    },
    {
        "name": "San Francisco",
        "url": os.environ.get(
            "SF_PERMIT_URL", "https://data.sfgov.org/resource/i98e-djp9.json"
        ),
        # Verified field name from dataset metadata: Issued Date -> issued_date
        "date_field": os.environ.get("SF_DATE_FIELD", "issued_date"),
    },
]


def iso_since(days: int = 7) -> str:
    """Return a Socrata-friendly ISO datetime string for midnight `days` ago.

    Format: YYYY-MM-DDT00:00:00 (no timezone offset) to match Socrata $where expectations.
    Uses timezone-aware now to compute the date, then emits a date-only timestamp at 00:00:00.
    """
    since_dt = datetime.now(timezone.utc) - timedelta(days=days)
    since_date = since_dt.date()
    return f"{since_date.isoformat()}T00:00:00"


def fetch_permits(source: Dict[str, str], since_iso: str, limit: int = 10000) -> List[Dict[str, Any]]:
    """Fetch permits and normalize to a consistent minimal schema.

    Uses Socrata `$select` to request only candidate fields, then normalizes each
    record into the schema requested by the product:

      { city, permit_id, permit_type, description, value, issued_date, address }

    The function is intentionally minimal and does no retries.
    """
    url = source.get("url")
    date_field = source.get("date_field")

    # Use source-specific safe select lists to avoid Socrata rejecting unknown columns
    if source.get("name") == "Chicago":
        # Verified fields from Chicago dataset
        select_fields = ",".join([
            "id",
            "permit_",
            "permit_type",
            "work_description",
            "issue_date",
            "reported_cost",
            "street_number",
            "street_name",
            "street_direction",
            "street_suffix",
        ])
    elif source.get("name") == "San Francisco":
        # Verified fields from SF dataset
        select_fields = ",".join([
            "permit_number",
            "permit_type",
            "description",
            "issued_date",
            "estimated_cost",
            "primary_address",
            "street_number",
            "street_name",
        ])
    else:
        # fallback - minimal safe fields
        select_fields = ",".join(["id", "permit_", "permit_number", date_field])

    where_clause = f"{date_field} >= '{since_iso}'"
    params = {
        "$select": select_fields,
        "$where": where_clause,
        "$limit": limit,
        "$order": f"{date_field} DESC",
    }

    headers = {"Accept": "application/json"}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as exc:
        # If $select causes a 400, fall back to requesting without $select (minimal retry).
        status = getattr(exc.response, "status_code", None)
        if status == 400 and "$select" in params:
            try:
                params.pop("$select", None)
                resp = requests.get(url, params=params, headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc2:
                print(f"[error] Failed to fetch {source.get('name')} after dropping $select: {exc2}", file=sys.stderr)
                return []
        else:
            print(f"[error] HTTP error fetching {source.get('name')}: {exc}", file=sys.stderr)
            return []
    except requests.RequestException as exc:
        print(f"[error] Failed to fetch {source.get('name')}: {exc}", file=sys.stderr)
        return []
    except ValueError as exc:
        print(f"[error] Invalid JSON from {source.get('name')}: {exc}", file=sys.stderr)
        return []

    if not isinstance(data, list):
        # try to extract list if API wrapped result
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    data = v
                    break
        if not isinstance(data, list):
            print(f"[warn] Unexpected response format from {source.get('name')}", file=sys.stderr)
            return []

    normalized: List[Dict[str, Any]] = []
    for raw in data:
        # helper to pick first available field from candidates
        def pick(*keys):
            for k in keys:
                if k in raw and raw.get(k) not in (None, ""):
                    return raw.get(k)
            return None

        pid = pick("permit_", "permit_number", "id") or None
        ptype = pick("permit_type", "Permit Type")
        desc = pick("work_description", "description")
        value = pick("reported_cost", "estimated_cost")
        # Normalize issued_date: attempt to parse and emit RFC3339 with timezone
        issued_raw = pick(date_field, "issue_date", "issued_date")
        issued_norm = None
        if isinstance(issued_raw, str):
            try:
                parsed = datetime.fromisoformat(issued_raw)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                issued_norm = parsed.isoformat()
            except Exception:
                # leave original string if parsing fails
                issued_norm = issued_raw
        elif isinstance(issued_raw, datetime):
            dt = issued_raw
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            issued_norm = dt.isoformat()

        # Build a compact address string from available parts
        addr = pick("address", "primary_address")
        if not addr:
            sn = pick("street_number") or ""
            sname = pick("street_name") or ""
            sfx = pick("street_suffix") or ""
            sdir = pick("street_direction") or ""
            parts = [sn, sdir, sname, sfx]
            addr = " ".join([p for p in parts if p]) or None

        # ensure permit_id present and also set 'id' for backward compatibility
        permit_id = pid or f"{source.get('name')}-{pick('id') or ''}"

        # compute numeric value safely
        num_value = None
        if isinstance(value, (int, float)):
            try:
                num_value = float(value)
            except Exception:
                num_value = None
        elif isinstance(value, str):
            try:
                cleaned = value.replace("$", "").replace(",", "").strip()
                if cleaned != "":
                    num_value = float(cleaned)
            except Exception:
                num_value = None

        # intelligence fields
        is_high_value = bool(num_value is not None and num_value >= 250000)

        # deal_score: tiered base (not additive) for value, plus keyword boosts
        deal_score = 0
        if num_value is not None:
            if num_value >= 1000000:
                deal_score = 8
            elif num_value >= 250000:
                deal_score = 3

        dlow = (desc or "").lower()
        if "new" in dlow or "addition" in dlow:
            deal_score += 2
        if "commercial" in dlow:
            deal_score += 2

        # vertical tags mapping (deterministic)
        vertical_tags: List[str] = []
        def add_tag(t: str):
            if t not in vertical_tags:
                vertical_tags.append(t)

        if "roof" in dlow:
            add_tag("Roofing")
        if "solar" in dlow:
            add_tag("Solar")
        if "electrical" in dlow:
            add_tag("Electrical")
        if "hvac" in dlow:
            add_tag("HVAC")
        if "demolition" in dlow or "demo" in dlow:
            add_tag("Demolition")
        if "new" in dlow:
            add_tag("New Construction")

        # primary_vertical: first matched tag or 'General'
        primary_vertical = vertical_tags[0] if vertical_tags else "General"

        # opportunity_class based on tiers
        if num_value is not None and num_value >= 1000000:
            opportunity_class = "MAJOR"
        elif num_value is not None and num_value >= 250000:
            opportunity_class = "MID"
        else:
            opportunity_class = "MINOR"

        normalized.append({
            "city": source.get("name"),
            "permit_id": permit_id,
            "id": permit_id,
            "permit_type": ptype,
            "description": desc,
            "value": num_value,
            "issued_date": issued_norm,
            "address": addr,
            "is_high_value": is_high_value,
            "deal_score": deal_score,
            "vertical_tags": vertical_tags,
            "primary_vertical": primary_vertical,
            "opportunity_class": opportunity_class,
        })

    return normalized


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
            # Ensure any datetime objects inside the record are timezone-aware
            for k, v in list(rec.items()):
                if isinstance(v, datetime) and v.tzinfo is None:
                    rec[k] = v.replace(tzinfo=timezone.utc)

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
        # Write the enhanced normalized records directly. The records list already
        # contains the enhanced schema (city, permit_id, permit_type, description,
        # value, issued_date, address, is_high_value, deal_score, vertical_tags,
        # primary_vertical, opportunity_class).
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "items": records}, fh, ensure_ascii=False, indent=2)
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
