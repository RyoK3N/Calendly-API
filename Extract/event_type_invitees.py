"""
event_type_invitees.py
──────────────────────
Count invitees for a Calendly event-type and export them to CSV.

Usage examples
==============
# CLI
python event_type_invitees.py --slug cleverly-introduction-cold-email --days 180

# Notebook
!python event_type_invitees.py cleverly-introduction-cold-email --days 90

# Import
from event_type_invitees import run
run("cleverly-introduction-cold-email", days=365)
"""

from __future__ import annotations
import os, sys, re, time, argparse, datetime as dt
from urllib.parse import urljoin

import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ────────────────────────── CONFIG ──────────────────────────
BASE          = "https://api.calendly.com/"
TOKEN         = os.getenv("CALENDLY_API_KEY")          # Personal-access token
REQUEST_TIMEOUT = 120                                  # seconds per request
MAX_RETRIES     = 3                                    # per request
DEFAULT_SLUG    = "cleverly-introduction-cold-email-international"   # fallback identifier

HEADERS = {
    "Authorization": f"Bearer {TOKEN or ''}",
    "Accept":        "application/json",
    "Content-Type":  "application/json",
}

# ──────────────────────── HTTP WRAPPERS ─────────────────────
def _get(url: str, params: dict, attempt: int = 1) -> dict:
    """
    Robust GET with timeout + retry.
    """
    try:
        resp = requests.get(url, headers=HEADERS,
                            params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ReadTimeout:
        if attempt >= MAX_RETRIES:
            raise
        backoff = 2 ** attempt
        time.sleep(backoff)
        return _get(url, params, attempt + 1)

def _paginate(url: str, params: dict) -> list[dict]:
    """
    Walk Calendly pagination until done; returns full collection.
    """
    out: list[dict] = []
    while url:
        page = _get(url, params)
        out.extend(page["collection"])
        url = page.get("pagination", {}).get("next_page")
        params = None          # only on first request
    return out

# ─────────────────────── Calendly HELPERS ──────────────────
def current_user_and_org() -> tuple[str, str]:
    me = _get(urljoin(BASE, "users/me"), {})
    res = me["resource"]
    return res["uri"], res["current_organization"]

def get_event_type(org_uri: str, ident: str) -> dict | None:
    """
    Locate an event-type by slug, UUID, or full URI within `org_uri`.
    """
    if ident.startswith("https://"):
        return _get(ident, {})["resource"]

    if re.fullmatch(r"[0-9a-f\-]{36}", ident):
        return _get(urljoin(BASE, f"event_types/{ident}"), {})["resource"]

    # assume slug
    ets = _paginate(urljoin(BASE, "event_types"),
                    {"organization": org_uri, "count": 100})
    return next((et for et in ets if et["slug"] == ident), None)

def list_org_events(org_uri: str, since_iso: str) -> list[dict]:
    """
    Return ACTIVE + CANCELED scheduled events in the org since `since_iso`.
    """
    events: list[dict] = []
    for status in ("active", "canceled"):
        events += _paginate(
            urljoin(BASE, "scheduled_events"),
            {
                "organization": org_uri,
                "status": status,
                "min_start_time": since_iso,
                "count": 100,
            },
        )
    return events

def list_invitees(event_uuid: str) -> list[dict]:
    return _paginate(
        urljoin(BASE, f"scheduled_events/{event_uuid}/invitees"),
        {"count": 100},
    )

# ───────────────────────── MAIN LOGIC ──────────────────────
def run(ident: str, days: int = 365) -> None:
    if not TOKEN:
        sys.exit("❌  CALENDLY_API_KEY must be set in your environment")

    _, org_uri = current_user_and_org()
    et = get_event_type(org_uri, ident)
    if not et:
        sys.exit(f"❌  No event-type \"{ident}\" found in this org")

    since_ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    since_iso = since_ts.isoformat(timespec="seconds")

    print(f"\n📄  Event-type: {et['name']}   (slug={et['slug']})")
    print(f"    Window    : events since {since_iso}Z\n")

    # 1️⃣  Fetch & filter events
    events = [ev for ev in list_org_events(org_uri, since_iso + "Z")
              if ev["event_type"] == et["uri"]]
    print(f"📅  Matching events found: {len(events)}")

    # 2️⃣  Collect invitees
    rows: list[dict] = []
    for ev in events:
        ev_uuid = ev["uri"].split("/")[-1]
        for iv in list_invitees(ev_uuid):
            rows.append({
                "event_uuid": ev_uuid,
                "event_start": ev["start_time"],
                "event_end": iv.get("end_time"),
                "location": iv.get("location"),
                "event_created": iv.get("created_at"),
                "status": iv.get("status"),
                "canceled_by": iv.get("canceled_by"),
                "cancellation_reason": iv.get("cancellation_reason"), 
                "invitee_uuid": iv["uri"].split("/")[-1],
                "user_name": iv.get("user_name"),
                "user_email": iv.get("user_email"),
                "team": iv.get("team"),
                "invitee_name": iv.get("invitee_name"),
                "invitee_first_name": iv.get("invitee_first_name"), 
                "invitee_last_name": iv.get("invitee_last_name"),
                "invitee_email": iv.get("invitee_email"),
                "invitee_time_zone": iv.get("invitee_time_zone"),
                "invitee_accepted_marketing_emails": iv.get("invitee_accepted_marketing_emails"),
                "text_reminder_number": iv.get("text_reminder_number"),
                "event_type_name": iv.get("event_type_name"),
                "question_1": iv.get("question_1"),
                "response_1": iv.get("response_1"),
                "question_2": iv.get("question_2"), 
                "response_2": iv.get("response_2"),
                "question_3": iv.get("question_3"),
                "response_3": iv.get("response_3"),
                "question_4": iv.get("question_4"),
                "response_4": iv.get("response_4"),
                "question_5": iv.get("question_5"),
                "response_5": iv.get("response_5"),
                "utm_campaign": iv.get("utm_campaign"),
                "utm_source": iv.get("utm_source"),
                "utm_medium": iv.get("utm_medium"),
                "utm_term": iv.get("utm_term"),
                "utm_content": iv.get("utm_content"),
                "salesforce_uuid": iv.get("salesforce_uuid"),
                "event_price": iv.get("event_price"),
                "payment_currency": iv.get("payment_currency"),
                "guest_emails": iv.get("guest_emails"),
                "invitee_reconfirmed": iv.get("invitee_reconfirmed"),
                "marked_as_no_show": iv.get("marked_as_no_show"),
                "meeting_notes": iv.get("meeting_notes"),
                "group": iv.get("group"),
                "invitee_scheduled_by": iv.get("invitee_scheduled_by"),
                "scheduling_method": iv.get("scheduling_method")
            })

    # 3️⃣  DataFrame ➜ CSV
    out_dir = "./data/downloads/"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    df = pd.DataFrame(rows)
    print(f"\n👥  Total invitees in window: {df.shape[0]}")
    if not df.empty:
        print(df.head())   # preview

    out_file = f"invitees_{et['slug']}.csv"
    outpath = os.path.join(out_dir,out_file)
    df.to_csv(outpath, index=False)
    print(f"\nSaved {out_file}")

# ───────────────────── CLI ENTRY-POINT ─────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug")
    ap.add_argument("--uuid")
    ap.add_argument("--days", type=int, default=365,
                    help="look-back window in days (default 365)")
    args, extra = ap.parse_known_args()

    # grab first positional arg not starting with "-" (Jupyter passes ―f=…)
    positional = next((a for a in extra if not a.startswith("-")), None)

    ident = args.slug or args.uuid or positional or DEFAULT_SLUG
    run(ident.strip(), args.days)
