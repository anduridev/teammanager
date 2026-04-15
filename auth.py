"""Authentication & role management helpers."""

import json
import os
from functools import wraps

from flask import session, redirect, url_for, request

from config import APP_CONFIG_FILE


# ── Config Load / Save ──


def load_app_config() -> dict:
    """Load app_config.json with roles and per-manager settings."""
    if os.path.exists(APP_CONFIG_FILE):
        try:
            with open(APP_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"superadmin": "TFS\\subbareddy", "managers": []}


def save_app_config(config: dict):
    """Save app_config.json."""
    with open(APP_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


# ── Role Checks ──


def get_current_user() -> dict | None:
    """Return the logged-in user dict from session, or None."""
    return session.get("user")


def is_superadmin(user: dict | None = None) -> bool:
    if user is None:
        user = get_current_user()
    if not user:
        return False
    config = load_app_config()
    sa = config.get("superadmin", "")
    unique = user.get("uniqueName", "")
    return unique.lower() == sa.lower()


def is_manager(user: dict | None = None) -> bool:
    if user is None:
        user = get_current_user()
    if not user:
        return False
    config = load_app_config()
    unique = user.get("uniqueName", "")
    for m in config.get("managers", []):
        if m.get("uniqueName", "").lower() == unique.lower():
            return True
    return False


def get_user_role(user: dict | None = None) -> str:
    """Return 'superadmin', 'manager', or 'member'."""
    if is_superadmin(user):
        return "superadmin"
    if is_manager(user):
        return "manager"
    return "member"


# ── Per-Manager Team & Projects ──


def _find_manager_entry(unique_name: str) -> dict | None:
    """Find a manager's entry in app_config by uniqueName."""
    config = load_app_config()
    for m in config.get("managers", []):
        if m.get("uniqueName", "").lower() == unique_name.lower():
            return m
    return None


def get_manager_team(user: dict | None = None) -> list[dict]:
    """Get the team members for the current manager.
    - Superadmin: returns all members across all managers (deduplicated).
    - Manager: returns only their own team.
    - Member: returns empty list.
    """
    if user is None:
        user = get_current_user()
    if not user:
        return []

    config = load_app_config()

    if is_superadmin(user):
        # Superadmin sees all managers' teams combined (deduplicated)
        seen = set()
        all_members = []
        # Check if superadmin has their own team
        sa_entry = _find_manager_entry(config.get("superadmin", ""))
        entries = config.get("managers", [])
        if sa_entry is None:
            # Also check superadmin's own config stored at top level
            sa_team = config.get("superadmin_team", [])
            for m in sa_team:
                key = (m.get("uniqueName") or m.get("displayName", "")).lower()
                if key and key not in seen:
                    seen.add(key)
                    all_members.append(m)
        for mgr in entries:
            for m in mgr.get("team", []):
                key = (m.get("uniqueName") or m.get("displayName", "")).lower()
                if key and key not in seen:
                    seen.add(key)
                    all_members.append(m)
        all_members.sort(key=lambda x: x.get("displayName", "").lower())
        return all_members

    if is_manager(user):
        entry = _find_manager_entry(user.get("uniqueName", ""))
        if entry:
            return entry.get("team", [])
    return []


def save_manager_team(user: dict, members: list[dict]):
    """Save team members for a specific manager."""
    config = load_app_config()
    unique = user.get("uniqueName", "")

    if is_superadmin(user):
        # Superadmin's team stored at top level
        config["superadmin_team"] = members
        save_app_config(config)
        return

    for mgr in config.get("managers", []):
        if mgr.get("uniqueName", "").lower() == unique.lower():
            mgr["team"] = members
            save_app_config(config)
            return


def get_manager_projects(user: dict | None = None) -> list:
    """Get the project+sprint combos a manager is tracking.
    Returns list of dicts: [{"project": "...", "sprint": "..."}]
    or legacy list of strings (backward compatible).
    """
    if user is None:
        user = get_current_user()
    if not user:
        return []

    config = load_app_config()

    if is_superadmin(user):
        return config.get("superadmin_projects", [])

    entry = _find_manager_entry(user.get("uniqueName", ""))
    if entry:
        return entry.get("projects", [])
    return []


def save_manager_projects(user: dict, projects: list):
    """Save project+sprint list for a specific manager.
    Accepts list of dicts: [{"project": "...", "sprint": "..."}]
    or list of strings (backward compatible).
    """
    config = load_app_config()
    unique = user.get("uniqueName", "")

    if is_superadmin(user):
        config["superadmin_projects"] = projects
        save_app_config(config)
        return

    for mgr in config.get("managers", []):
        if mgr.get("uniqueName", "").lower() == unique.lower():
            mgr["projects"] = projects
            save_app_config(config)
            return


def get_manager_pbi_prefix(user: dict | None = None) -> str:
    """Get the PBI code prefix for the current user.
    - Superadmin: uses superadmin_pbi_prefix
    - Manager: uses their own pbi_prefix
    - Member: inherits prefix from the manager whose team they belong to
    """
    if user is None:
        user = get_current_user()
    if not user:
        return ""

    config = load_app_config()

    if is_superadmin(user):
        return config.get("superadmin_pbi_prefix", "")

    # Check if user is a manager
    entry = _find_manager_entry(user.get("uniqueName", ""))
    if entry:
        return entry.get("pbi_prefix", "")

    # Member: find which manager's team they belong to
    unique = user.get("uniqueName", "").lower()
    display = user.get("displayName", "").lower()
    user_id = user.get("id", "").lower()

    for mgr in config.get("managers", []):
        for m in mgr.get("team", []):
            m_unique = m.get("uniqueName", "").lower()
            m_display = m.get("displayName", "").lower()
            m_id = m.get("id", "").lower()
            if (unique and m_unique == unique) or \
               (user_id and m_id == user_id) or \
               (display and m_display == display):
                return mgr.get("pbi_prefix", "")

    # Also check superadmin's team
    for m in config.get("superadmin_team", []):
        m_unique = m.get("uniqueName", "").lower()
        m_display = m.get("displayName", "").lower()
        m_id = m.get("id", "").lower()
        if (unique and m_unique == unique) or \
           (user_id and m_id == user_id) or \
           (display and m_display == display):
            return config.get("superadmin_pbi_prefix", "")

    return ""


def save_manager_pbi_prefix(user: dict, prefix: str):
    """Save PBI code prefix for a specific manager."""
    config = load_app_config()
    unique = user.get("uniqueName", "")

    if is_superadmin(user):
        config["superadmin_pbi_prefix"] = prefix
        save_app_config(config)
        return

    for mgr in config.get("managers", []):
        if mgr.get("uniqueName", "").lower() == unique.lower():
            mgr["pbi_prefix"] = prefix
            save_app_config(config)
            return


def get_all_manager_team_members() -> dict[str, list[dict]]:
    """Return a dict of manager uniqueName → their team members.
    Used by superadmin to see who has which team."""
    config = load_app_config()
    result = {}
    sa = config.get("superadmin", "")
    if config.get("superadmin_team"):
        result[sa] = config["superadmin_team"]
    for mgr in config.get("managers", []):
        result[mgr.get("uniqueName", "")] = mgr.get("team", [])
    return result


# ── Auth Decorator ──


def login_required(f):
    """Redirect to /login if not authenticated; return JSON 401 for API routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not get_current_user():
            if request.path.startswith("/api/"):
                from flask import jsonify
                return jsonify({"error": "Not authenticated"}), 401
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated
