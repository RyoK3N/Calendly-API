"""
organisation_snapshot.py
────────────────
Quick org overview:
• Org name / URI / plan / timezone
• List of members with role, name, email

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
    "Accept":        "application/json",
    "Content-Type":  "application/json",
}

# ───────────────── helpers ──────────────────
def _get(url: str, params: dict | None = None) -> dict:
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        print(f"HTTP {r.status_code} → {r.text}", file=sys.stderr)
        raise RuntimeError("Calendly API call failed") from e
    return r.json()

def _paginate(url: str, params: dict) -> list[dict]:
    items: list[dict] = []
    while url:
        page = _get(url, params)
        items.extend(page["collection"])
        url = page.get("pagination", {}).get("next_page")
        params = None        # params only on first call
    return items

# ───────────────── core ─────────────────────
def current_profile() -> dict:
    return _get(urljoin(BASE, "users/me"))["resource"]

def org_details(org_uri: str) -> dict:
    return _get(org_uri)["resource"]

def org_members(org_uri: str) -> list[dict]:
    """Return [{name,email,role}] in one paginated sweep."""
    url    = urljoin(BASE, "organization_memberships")
    params = {
        "organization": org_uri,
        "include":      "user",    # <-- embeds user profile!
        "count":        100,
    }
    memberships = _paginate(url, params)
    users: list[dict] = []
    for m in memberships:
        u = m.get("user")
        if not u:               # deleted / inaccessible account
            continue
        users.append({
            "name":  u.get("name",  "—"),
            "email": u.get("email", "—"),
            "role":  m.get("role",  "—"),
        })
    return users

def main() -> None:
    me       = current_profile()
    org_uri  = me["current_organization"]
    org      = org_details(org_uri)

    print("🏢  Organisation")
    print("─────────────────────────────────────────────────────────")
    print(f"Name      : {org.get('name')}")
    print(f"URI       : {org_uri}")
    print(f"Plan      : {org.get('subscription_type', 'n/a')}")
    print(f"Timezone  : {org.get('timezone', 'n/a')}\n")

    members = org_members(org_uri)
    print(f"👤  Members ({len(members)})")
    print("─────────────────────────────────────────────────────────")
    for m in members:
        print(f"{m['name']:<30}  {m['email']:<30}  [{m['role']}]")

if __name__ == "__main__":
    main()
