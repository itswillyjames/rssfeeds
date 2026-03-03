#!/usr/bin/env python3
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime, timedelta, timezone, timezone
import json
import os
import time

HIGH_VALUE_THRESHOLD = 250000

KEYWORD_CATEGORIES = {
    "NEW_CONSTRUCTION": ["new", "ground up"],
    "ADDITION": ["addition", "expand"],
    "RENOVATION": ["remodel", "renovation", "alter"],
    "DEMOLITION": ["demo", "demolition"],
    "SOLAR": ["solar"],
    "ROOF": ["roof"]
}

ARBITRAGE_MAP = {
    "NEW_CONSTRUCTION": ["Lumber Supply", "Insurance", "Lending", "Equipment Rental"],
    "ADDITION": ["HVAC", "Electrical", "Roofing"],
    "RENOVATION": ["Interior Supply", "Flooring", "Windows"],
    "DEMOLITION": ["Waste Removal", "Heavy Equipment"],
    "SOLAR": ["Solar Installers", "Energy Financing"],
    "ROOF": ["Roofing Contractors", "Material Suppliers"]
}

class PermitRSSBuilder:

    def __init__(self, token=None):
        self.headers = {'X-App-Token': token} if token else {}

        self.cities = {
            "chicago": {
                "name": "Chicago",
                "endpoint": "https://data.cityofchicago.org/resource/ydr8-5enu.json",
                "filters": "permit_status='ISSUED'",
                "date": "issue_date",
                "title": "permit_type",
                "desc": "work_description",
                "value": "reported_cost",
                "id": "permit_",
                "address": "street_number street_direction street_name"
            },
            "sf": {
                "name": "San Francisco",
                "endpoint": "https://data.sfgov.org/resource/i98e-djp9.json",
                "filters": "current_status='issued'",
                "date": "issued_date",
                "title": "permit_type",
                "desc": "description",
                "value": "estimated_cost",
                "id": "permit_number",
                "address": "location"
            }
        }

    def fetch(self, city_key, days=7, limit=100):
        city = self.cities[city_key]
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')

        params = {
            "$limit": limit,
            "$order": f"{city['date']} DESC",
            "$where": f"{city['filters']} AND {city['date']} >= '{cutoff}'"
        }

        try:
            r = requests.get(city["endpoint"], headers=self.headers, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except:
            return []

    def detect_category(self, text):
        text = text.lower()
        for category, keywords in KEYWORD_CATEGORIES.items():
            for k in keywords:
                if k in text:
                    return category
        return "GENERAL"

    def score_permit(self, value, category, description):
        score = 0
        urgency = 0

        if value > HIGH_VALUE_THRESHOLD:
            score += 3
        if value > 1000000:
            score += 5
        if category in ["NEW_CONSTRUCTION", "ADDITION"]:
            score += 2
        if "commercial" in description.lower():
            score += 2
        if "urgent" in description.lower():
            urgency += 2

        return score, urgency

    def build_address(self, permit, addr_field):
        if " " in addr_field:
            parts = addr_field.split()
            return " ".join(str(permit.get(p, "")) for p in parts if permit.get(p))
        return permit.get(addr_field, "Address N/A")

    def normalize(self, city_key, permit):
        city = self.cities[city_key]
        value = float(permit.get(city["value"], 0) or 0)
        desc = str(permit.get(city["desc"], ""))

        category = self.detect_category(desc)
        score, urgency = self.score_permit(value, category, desc)

        return {
            "city": city["name"],
            "permit_type": permit.get(city["title"], ""),
            "description": desc,
            "value": value,
            "category": category,
            "deal_score": score,
            "urgency_score": urgency,
            "is_high_value": value > HIGH_VALUE_THRESHOLD,
            "arbitrage_tags": ARBITRAGE_MAP.get(category, []),
            "issued_date": permit.get(city["date"], ""),
            "address": self.build_address(permit, city["address"])
        }

    def build_rss(self, filename, data):
        fg = FeedGenerator()
        fg.title("Permit Intelligence Feed")
        fg.link(href="https://example.com")
        fg.description("Structured permit intelligence feed")
        fg.lastBuildDate(datetime.now(timezone.utc))

        for item in data:
            fe = fg.add_entry()
            fe.title(f"[{item['city']}] {item['permit_type']} - {item['address']}")
            fe.description(
                f"Category: {item['category']} | "
                f"Value: ${item['value']} | "
                f"Score: {item['deal_score']} | "
                f"Tags: {', '.join(item['arbitrage_tags'])}"
            )
            fe.guid(f"{item['city']}-{item['issued_date']}-{item['address']}")
            fe.pubDate(datetime.now(timezone.utc))

        fg.rss_file(filename)

    def run(self):
        all_normalized = []
        high_value = []

        for city_key in self.cities:
            data = self.fetch(city_key)
            for p in data:
                normalized = self.normalize(city_key, p)
                all_normalized.append(normalized)
                if normalized["is_high_value"]:
                    high_value.append(normalized)
            time.sleep(1)

        with open("master_permit_feed.json", "w") as f:
            json.dump(all_normalized, f, indent=2)

        with open("high_value_permits.json", "w") as f:
            json.dump(high_value, f, indent=2)

        self.build_rss("master_permit_feed.xml", all_normalized)
        self.build_rss("high_value_permits.xml", high_value)

if __name__ == "__main__":
    token = os.environ.get("SOCRATA_TOKEN")
    PermitRSSBuilder(token).run()
