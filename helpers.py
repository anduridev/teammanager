"""Utility functions — team loading, matching, sprint calculations."""

import json
import os
import re
from datetime import date, datetime, timedelta

from config import TEAM_FILE, HOURS_PER_DAY


# ── Hours Parsing ──


def parse_hours_from_title(title: str) -> float:
    """Parse planned hours from task title pattern like 'Task name -3hrs'."""
    if not title:
        return 0
    m = re.search(r'-\s*(\d+(?:\.\d+)?)\s*h(?:rs?)?(?:\s|$)', title, re.IGNORECASE)
    return float(m.group(1)) if m else 0


# ── Team Data ──


def load_team_data(team_override: list[dict] | None = None) -> list[dict]:
    """Load team member objects. Uses team_override if provided,
    otherwise falls back to team_members.json (legacy)."""
    if team_override is not None:
        return team_override
    if os.path.exists(TEAM_FILE):
        with open(TEAM_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if data and isinstance(data[0], str):
                return [{"displayName": name, "uniqueName": "", "id": ""} for name in data]
            return data
    return []


def save_team(members: list[dict]):
    """Save selected team members to local JSON file (legacy fallback)."""
    with open(TEAM_FILE, "w", encoding="utf-8") as f:
        json.dump(members, f, indent=2, ensure_ascii=False)


def get_team_members(team_override: list[dict] | None = None) -> list[str]:
    """Get display names list."""
    return [m["displayName"] for m in load_team_data(team_override)]


def get_team_lookup(team_override: list[dict] | None = None) -> dict:
    """Build lookup tables keyed by uniqueName, id, displayName → displayName."""
    by_unique, by_id, by_display = {}, {}, {}
    for m in load_team_data(team_override):
        dn = m["displayName"]
        if m.get("uniqueName"):
            by_unique[m["uniqueName"].lower()] = dn
        if m.get("id"):
            by_id[m["id"].lower()] = dn
        by_display[dn.lower()] = dn
    return {"unique": by_unique, "id": by_id, "display": by_display}


# ── Assignee Extraction & Matching ──


def get_assignee_info(fields: dict) -> tuple[str, str, str]:
    """Extract (displayName, uniqueName, id) from AssignedTo field."""
    assigned = fields.get("System.AssignedTo")
    if isinstance(assigned, dict):
        return (
            assigned.get("displayName", ""),
            assigned.get("uniqueName", ""),
            assigned.get("id", ""),
        )
    if isinstance(assigned, str) and assigned:
        if "\\" in assigned:
            return ("", assigned, "")
        return (assigned, "", "")
    return ("", "", "")


def get_assignee(fields: dict) -> str:
    """Get display name for showing in UI."""
    name, unique, uid = get_assignee_info(fields)
    if name:
        return name
    if unique:
        lookup = get_team_lookup()
        match = lookup["unique"].get(unique.lower())
        if match:
            return match
        return unique.rsplit("\\", 1)[-1]
    return ""


def match_member(assignee_name: str, assignee_unique: str = "", assignee_id: str = "") -> str | None:
    """Match assignee to a team member. Strict matching."""
    lookup = get_team_lookup()

    if assignee_unique:
        match = lookup["unique"].get(assignee_unique.lower())
        if match:
            return match

    if assignee_id:
        match = lookup["id"].get(assignee_id.lower())
        if match:
            return match

    if assignee_name:
        match = lookup["display"].get(assignee_name.lower())
        if match:
            return match

    if assignee_name and "\\" in assignee_name:
        match = lookup["unique"].get(assignee_name.lower())
        if match:
            return match

    if assignee_name and not assignee_unique:
        an = assignee_name.lower()
        for uname, dn in lookup["unique"].items():
            username = uname.rsplit("\\", 1)[-1]
            if username == an:
                return dn

    return None


def find_unique_name(display_name: str) -> str | None:
    """Find the uniqueName for a given displayName from team data."""
    for m in load_team_data():
        if m["displayName"].lower() == display_name.lower():
            return m.get("uniqueName", "")
    return None


# ── Work Item Helpers ──


def get_parent_id(work_item: dict) -> int | None:
    for rel in work_item.get("relations", []) or []:
        if rel.get("rel") == "System.LinkTypes.Hierarchy-Reverse":
            try:
                return int(rel.get("url", "").rsplit("/", 1)[-1])
            except (ValueError, IndexError):
                pass
    return None


# ── Sprint / Date Helpers ──


def count_working_days(start: date, end: date) -> int:
    """Count weekdays between two dates (inclusive)."""
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count


def get_sprint_day_info(sprint_info: dict) -> dict:
    """Calculate sprint day progress from start/end dates."""
    start_str = sprint_info.get("start_date", "")
    end_str = sprint_info.get("end_date", "")
    if not start_str or not end_str:
        return {"total_days": 10, "elapsed_days": 0, "remaining_days": 10, "pct_elapsed": 0}

    try:
        start = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
        end = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return {"total_days": 10, "elapsed_days": 0, "remaining_days": 10, "pct_elapsed": 0}

    today = date.today()
    total_days = count_working_days(start, end) or 1

    if today < start:
        elapsed = 0
    elif today > end:
        elapsed = total_days
    else:
        yesterday = today - timedelta(days=1)
        elapsed = count_working_days(start, yesterday) if yesterday >= start else 0

    remaining_days = max(0, total_days - elapsed)
    pct = round((elapsed / total_days) * 100, 1)

    if today < start:
        current_day = 0
    elif today > end:
        current_day = total_days
    else:
        current_day = elapsed + 1

    return {
        "total_days": total_days,
        "elapsed_days": elapsed,
        "remaining_days": remaining_days,
        "current_day": min(current_day, total_days),
        "pct_elapsed": pct,
    }


def get_current_sprint(ado) -> str | None:
    """Find the current sprint (timeFrame=current)."""
    try:
        sprints = ado.get_iterations()
        for s in sprints:
            if s.get("attributes", {}).get("timeFrame") == "current":
                return s["path"]
        if sprints:
            return sprints[-1]["path"]
    except Exception:
        pass
    return None
