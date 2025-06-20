"""
organisation_event_types.py
──────────────────
List all Calendly event-types that belong to the organisation
visible to the current Personal-Access Token (PAT).

"""

from __future__ import annotations
import os, sys, requests
from urllib.parse import urljoin
from dotenv import load_dotenv
load_dotenv()

BASE  = "https://api.calendly.com/"
TOKEN = os.getenv("CALENDLY_API_KEY")
if not TOKEN:
    sys.exit("❌  CALENDLY_API_KEY not set in environment")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept":        "application/json",
    "Content-Type":  "application/json",
}

# ───────────────── helpers ──────────────────
def _get(url: str, params: dict | None = None) -> dict:
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _paginate(url: str, params: dict) -> list[dict]:
    coll: list[dict] = []
    while url:
        page = _get(url, params)
        coll.extend(page["collection"])
        url   = page.get("pagination", {}).get("next_page")
        params = None                # only on first call
    return coll

# ───────────────── core ─────────────────────
def current_org_uri() -> str:
    me = _get(urljoin(BASE, "users/me"))["resource"]
    return me["current_organization"]        # e.g. https://api.calendly.com/organizations/BGEGELVJZ77USIF4

def list_event_types(org_uri: str) -> list[dict]:
    url    = urljoin(BASE, "event_types")
    params = {"organization": org_uri, "count": 100}
    return _paginate(url, params)

def main() -> None:
    org_uri = current_org_uri()
    evt     = list_event_types(org_uri)

    print(f"📄  Event-types in org {org_uri.split('/')[-1]} ({len(evt)})")
    print("────────────────────────────────────────────────────────────")
    for et in evt:
        mins = et.get("duration")
        print(f"{et['name']:<40}  slug={et['slug']:<30}  {mins:>3} min")

if __name__ == "__main__":
    main()
