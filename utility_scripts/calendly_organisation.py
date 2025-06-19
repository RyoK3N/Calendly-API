"""
calendly_organisation.py
──────────────────────
Show how many Calendly organisations the current PAT is a member of.
"""

from __future__ import annotations
import os, sys, requests
from urllib.parse import urljoin

BASE = "https://api.calendly.com/"
TOKEN = os.getenv("CALENDLY_API_KEY")
if not TOKEN:
    sys.exit("❌  CALENDLY_API_KEY not set in environment")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# ───────────────── helpers ──────────────────
def _get(url: str, params: dict | None = None) -> dict:
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _paginate(url: str, params: dict) -> list[dict]:
    items: list[dict] = []
    while url:
        page = _get(url, params)
        items.extend(page["collection"])
        url = page.get("pagination", {}).get("next_page")
        params = None
    return items

# ───────────────── core logic ───────────────
def count_organisations() -> int:
    # 1️⃣  who am I?
    me = _get(urljoin(BASE, "users/me"))["resource"]
    user_uri = me["uri"]

    # 2️⃣  memberships for this user
    url    = urljoin(BASE, "organization_memberships")
    params = {"user": user_uri, "count": 100}
    memberships = _paginate(url, params)

    # 3️⃣  unique org URIs
    org_uris = {m["organization"] for m in memberships}
    return len(org_uris)

if __name__ == "__main__":
    n = count_organisations()
    print(f"👥  Organisations visible to this PAT: {n}")
