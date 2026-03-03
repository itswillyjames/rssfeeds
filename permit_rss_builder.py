#!/usr/bin/env python3
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime, timedelta, timezone
import json
import os
import time

HIGH_VALUE_THRESHOLD = 250000

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

    def build_address(self, permit, addr_field):
        if " " in addr_field:
            parts = addr_field.split()
            return " ".join(str(permit.get(p, "")) for p in parts if permit.get(p))
        return permit.get(addr_field, "Address N/A")

    def build_rss(self, filename, data):
        fg = FeedGenerator()
        fg.title("Permit Intelligence Feed")
        fg.link(href="https://example.com")
        fg.description("Structured permit intelligence feed")
        fg.lastBuildDate(datetime.now(timezone.utc))

        for item in data:
            fe = fg.add_entry()
            fe.title(f"[{item['city']}] {item['permit_type']} - {item['address']}")
            fe.description(f"Value: ${item['value']}")
            fe.guid(f"{item['city']}-{item['issued_date']}-{item['address']}")
            fe.pubDate(datetime.now(timezone.utc))

        fg.rss_file(filename)

    def run(self):
        all_data = []

        for city_key in self.cities:
            data = self.fetch(city_key)
            city = self.cities[city_key]
            for p in data:
                value = float(p.get(city["value"], 0) or 0)
                normalized = {
                    "city": city["name"],
                    "permit_type": p.get(city["title"], ""),
                    "value": value,
                    "issued_date": p.get(city["date"], ""),
                    "address": self.build_address(p, city["address"])
                }
                all_data.append(normalized)
            time.sleep(1)

        with open("master_permit_feed.json", "w") as f:
            json.dump(all_data, f, indent=2)

        self.build_rss("master_permit_feed.xml", all_data)

if __name__ == "__main__":
    token = os.environ.get("SOCRATA_TOKEN")
    PermitRSSBuilder(token).run()
