"""
Azure DevOps Agent — Web UI powered by Flask.
Sidebar analytics query TFS directly (zero AI cost).
Only the chat input uses OpenAI for complex/custom questions.
"""

import json
import os
import sys
from datetime import datetime, date
from flask import Flask, render_template, request, jsonify, session
from openai import OpenAI
from dotenv import load_dotenv
from azure_devops_client import AzureDevOpsClient

load_dotenv()

# ── Configuration ───────────────────────────────────────────────────

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
AZURE_DEVOPS_ORG_URL = os.environ.get("AZURE_DEVOPS_ORG_URL")
AZURE_DEVOPS_PAT = os.environ.get("AZURE_DEVOPS_PAT")
AZURE_DEVOPS_PROJECT = os.environ.get("AZURE_DEVOPS_PROJECT")

if not all([AZURE_DEVOPS_ORG_URL, AZURE_DEVOPS_PAT, AZURE_DEVOPS_PROJECT]):
    print("Error: Missing Azure DevOps environment variables.")
    sys.exit(1)

ai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
ado = AzureDevOpsClient(AZURE_DEVOPS_ORG_URL, AZURE_DEVOPS_PAT, AZURE_DEVOPS_PROJECT)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(24).hex())


@app.before_request
def _begin_cache():
    """Start request-scoped cache — TFS calls within this request share data."""
    ado._cache.begin()


@app.after_request
def _end_cache(response):
    """End request-scoped cache — discard all cached data."""
    ado._cache.end()
    return response

# ── Team Roster (loaded from local JSON file) ──────────────────────
# JSON format: [{"displayName": "...", "uniqueName": "...", "id": "..."}, ...]

TEAM_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "team_members.json")
HOURS_PER_DAY = 8


def _load_team_data() -> list[dict]:
    """Load full team member objects from JSON file."""
    if os.path.exists(TEAM_FILE):
        with open(TEAM_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Support old format (list of strings) — auto-migrate
            if data and isinstance(data[0], str):
                return [{"displayName": name, "uniqueName": "", "id": ""} for name in data]
            return data
    return []


def _save_team(members: list[dict]):
    """Save selected team members to local JSON file."""
    with open(TEAM_FILE, "w", encoding="utf-8") as f:
        json.dump(members, f, indent=2, ensure_ascii=False)


def get_team_members() -> list[str]:
    """Get display names for backward compatibility."""
    return [m["displayName"] for m in _load_team_data()]


def _get_team_lookup() -> dict:
    """Build lookup tables keyed by uniqueName and id → displayName.
    Strict matching only — no partial displayName matching."""
    by_unique = {}  # uniqueName (lowered) → displayName
    by_id = {}      # id (lowered) → displayName
    by_display = {} # displayName (lowered) → displayName (exact only)
    for m in _load_team_data():
        dn = m["displayName"]
        if m.get("uniqueName"):
            by_unique[m["uniqueName"].lower()] = dn
        if m.get("id"):
            by_id[m["id"].lower()] = dn
        by_display[dn.lower()] = dn
    return {"unique": by_unique, "id": by_id, "display": by_display}


# ── Helpers ──────────────────────────────────────────────────────────


def _get_assignee_info(fields: dict) -> tuple[str, str, str]:
    """Extract (displayName, uniqueName, id) from AssignedTo field.
    TFS v5.0 may return AssignedTo as a plain string (e.g. 'TFS\\sowjanya' or 'Sowjanya')
    instead of a dict. We handle both cases."""
    assigned = fields.get("System.AssignedTo")
    if isinstance(assigned, dict):
        return (
            assigned.get("displayName", ""),
            assigned.get("uniqueName", ""),
            assigned.get("id", ""),
        )
    if isinstance(assigned, str) and assigned:
        # Plain string — could be a uniqueName like 'TFS\\sowjanya' or a displayName
        # Check if it looks like a uniqueName (contains backslash)
        if "\\" in assigned:
            return ("", assigned, "")
        return (assigned, "", "")
    return ("", "", "")


def _get_assignee(fields: dict) -> str:
    """Get display name for showing in UI."""
    name, unique, uid = _get_assignee_info(fields)
    if name:
        return name
    # If we only have uniqueName, resolve it to displayName from team data
    if unique:
        lookup = _get_team_lookup()
        match = lookup["unique"].get(unique.lower())
        if match:
            return match
        # Show the part after backslash as fallback
        return unique.rsplit("\\", 1)[-1]
    return ""


def _match_member(assignee_name: str, assignee_unique: str = "", assignee_id: str = "") -> str | None:
    """Match assignee to a team member. Strict matching by uniqueName, id, or exact displayName.
    Also handles plain string AssignedTo values from older TFS."""
    lookup = _get_team_lookup()

    # 1. Exact match on uniqueName (most reliable — e.g. TFS\\sowjanya)
    if assignee_unique:
        match = lookup["unique"].get(assignee_unique.lower())
        if match:
            return match

    # 2. Exact match on id (GUID)
    if assignee_id:
        match = lookup["id"].get(assignee_id.lower())
        if match:
            return match

    # 3. Exact match on displayName
    if assignee_name:
        match = lookup["display"].get(assignee_name.lower())
        if match:
            return match

    # 4. If assignee_name looks like a uniqueName (has backslash), try that
    if assignee_name and "\\" in assignee_name:
        match = lookup["unique"].get(assignee_name.lower())
        if match:
            return match

    # 5. If we have a plain string, check if it matches any uniqueName's username part
    #    e.g. "sowjanya" should match "TFS\\sowjanya" but NOT "TFS\\anduri" (displayName "Anduri Sowjanya")
    if assignee_name and not assignee_unique:
        an = assignee_name.lower()
        for uname, dn in lookup["unique"].items():
            # Extract username after backslash: "TFS\\sowjanya" → "sowjanya"
            username = uname.rsplit("\\", 1)[-1]
            if username == an:
                return dn

    return None


def _get_parent_id(work_item: dict) -> int | None:
    for rel in work_item.get("relations", []) or []:
        if rel.get("rel") == "System.LinkTypes.Hierarchy-Reverse":
            try:
                return int(rel.get("url", "").rsplit("/", 1)[-1])
            except (ValueError, IndexError):
                pass
    return None


def _count_working_days(start: date, end: date) -> int:
    """Count weekdays between two dates (inclusive)."""
    count = 0
    current = start
    from datetime import timedelta
    while current <= end:
        if current.weekday() < 5:  # Mon-Fri
            count += 1
        current += timedelta(days=1)
    return count


def _get_sprint_day_info(sprint_info: dict) -> dict:
    """Calculate sprint day progress from start/end dates."""
    start_str = sprint_info.get("start_date", "")
    end_str = sprint_info.get("end_date", "")
    if not start_str or not end_str:
        return {"total_days": 10, "elapsed_days": 0, "remaining_days": 10, "pct_elapsed": 0}

    try:
        # TFS dates: "2026-03-10T00:00:00Z" or "2026-03-10"
        start = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
        end = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return {"total_days": 10, "elapsed_days": 0, "remaining_days": 10, "pct_elapsed": 0}

    today = date.today()
    total_days = _count_working_days(start, end)
    if total_days == 0:
        total_days = 1

    if today < start:
        elapsed = 0
    elif today > end:
        elapsed = total_days
    else:
        # Elapsed = completed days only (before today, not including today)
        from datetime import timedelta
        yesterday = today - timedelta(days=1)
        if yesterday < start:
            elapsed = 0
        else:
            elapsed = _count_working_days(start, yesterday)

    # Remaining includes today (today is not yet done)
    remaining_days = max(0, total_days - elapsed)
    pct = round((elapsed / total_days) * 100, 1)

    return {
        "total_days": total_days,
        "elapsed_days": elapsed,
        "remaining_days": remaining_days,
        "pct_elapsed": pct,
    }


def _get_current_sprint() -> str | None:
    """Find the current sprint (timeFrame=current)."""
    try:
        sprints = ado.get_iterations()
        for s in sprints:
            if s.get("attributes", {}).get("timeFrame") == "current":
                return s["path"]
        # Fallback: return last sprint
        if sprints:
            return sprints[-1]["path"]
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════════════
#  DIRECT ANALYTICS — no OpenAI, just TFS queries
# ══════════════════════════════════════════════════════════════════════


def _build_team_workload(sprint: str, sprint_days: int = 10) -> dict:
    capacity_per_member = sprint_days * HOURS_PER_DAY

    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task' "
        f"ORDER BY [System.AssignedTo]"
    )
    all_tasks = ado.query_work_items(wiql)

    team_data = {}
    for member in get_team_members():
        team_data[member] = {
            "member": member,
            "total_allocated_hours": 0, "remaining_hours": 0,
            "completed_hours": 0, "task_count": 0,
            "done_count": 0, "in_progress_count": 0, "new_count": 0,
            "capacity_hours": capacity_per_member,
        }

    for wi in all_tasks:
        fields = wi["fields"]
        a_name, a_unique, a_id = _get_assignee_info(fields)
        if not a_name:
            continue
        matched = _match_member(a_name, a_unique, a_id)
        if not matched:
            continue

        state = fields.get("System.State", "New")
        original = fields.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0
        remaining = fields.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0
        completed = fields.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0
        allocated = original if original else remaining

        m = team_data[matched]
        m["total_allocated_hours"] += allocated
        m["remaining_hours"] += remaining
        m["completed_hours"] += completed
        m["task_count"] += 1
        if state in ("Closed", "Done", "Resolved"):
            m["done_count"] += 1
        elif state in ("Active", "In Progress"):
            m["in_progress_count"] += 1
        else:
            m["new_count"] += 1

    for m in team_data.values():
        m["free_hours"] = max(0, m["capacity_hours"] - m["total_allocated_hours"])
        m["completion_pct"] = (
            round((m["completed_hours"] / m["total_allocated_hours"]) * 100, 1)
            if m["total_allocated_hours"] > 0 else 0
        )

    total_capacity = capacity_per_member * len(get_team_members())
    total_alloc = sum(m["total_allocated_hours"] for m in team_data.values())

    return {
        "sprint": sprint, "sprint_days": sprint_days,
        "team_summary": {
            "total_members": len(get_team_members()),
            "total_capacity_hours": total_capacity,
            "total_allocated_hours": total_alloc,
            "total_remaining_hours": sum(m["remaining_hours"] for m in team_data.values()),
            "total_completed_hours": sum(m["completed_hours"] for m in team_data.values()),
            "total_free_hours": sum(m["free_hours"] for m in team_data.values()),
            "utilization_pct": round((total_alloc / total_capacity) * 100, 1) if total_capacity else 0,
        },
        "members": list(team_data.values()),
    }


def _build_sprint_summary(sprint: str) -> dict:
    pbi_wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Product Backlog Item'"
    )
    pbis = ado.query_work_items(pbi_wiql)

    task_wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task'"
    )
    tasks = ado.query_work_items(task_wiql)

    pbi_states = {}
    for wi in pbis:
        s = wi["fields"].get("System.State", "New")
        pbi_states[s] = pbi_states.get(s, 0) + 1

    task_states = {}
    total_original = total_remaining = total_completed = 0
    for wi in tasks:
        f = wi["fields"]
        s = f.get("System.State", "New")
        task_states[s] = task_states.get(s, 0) + 1
        total_original += f.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0
        total_remaining += f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0
        total_completed += f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0

    total_effort = total_original if total_original else (total_remaining + total_completed)
    return {
        "sprint": sprint,
        "pbis": {"total": len(pbis), "by_state": pbi_states},
        "tasks": {"total": len(tasks), "by_state": task_states},
        "hours": {
            "original_estimate": total_original,
            "completed": total_completed,
            "remaining": total_remaining,
            "completion_pct": round((total_completed / total_effort) * 100, 1) if total_effort else 0,
        },
    }


def _build_tasks_by_state(sprint: str, states: list[str]) -> dict:
    state_filter = " OR ".join(f"[System.State] = '{s}'" for s in states)
    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task' "
        f"AND ({state_filter}) "
        f"ORDER BY [System.AssignedTo]"
    )
    results = ado.query_work_items(wiql)
    items = []
    for wi in results:
        f = wi["fields"]
        items.append({
            "id": wi["id"],
            "title": f.get("System.Title"),
            "state": f.get("System.State"),
            "assigned_to": _get_assignee(f),
            "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0,
            "completed_hours": f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0,
        })
    return {"sprint": sprint, "states": states, "count": len(items), "items": items}


def _find_unique_name(display_name: str) -> str | None:
    """Find the uniqueName for a given displayName from team data."""
    for m in _load_team_data():
        if m["displayName"].lower() == display_name.lower():
            return m.get("uniqueName", "")
    return None


def _build_member_tasks(member: str, sprint: str) -> dict:
    # Use uniqueName for WIQL query (exact match, no ambiguity)
    unique = _find_unique_name(member)
    if unique:
        filter_clause = f"[System.AssignedTo] = '{unique}'"
    else:
        filter_clause = f"[System.AssignedTo] = '{member}'"
    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task' "
        f"AND {filter_clause} "
        f"ORDER BY [System.State]"
    )
    tasks = ado.query_work_items(wiql)

    items = []
    total_remaining = total_completed = total_allocated = 0
    for wi in tasks:
        f = wi["fields"]
        original = f.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0
        remaining = f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0
        completed = f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0
        allocated = original if original else remaining
        items.append({
            "id": wi["id"], "title": f.get("System.Title"),
            "state": f.get("System.State"),
            "priority": f.get("Microsoft.VSTS.Common.Priority"),
            "allocated_hours": allocated,
            "remaining_hours": remaining, "completed_hours": completed,
            "parent_id": _get_parent_id(wi),
        })
        total_remaining += remaining
        total_completed += completed
        total_allocated += allocated

    return {
        "member": member, "sprint": sprint, "task_count": len(items),
        "total_allocated_hours": total_allocated,
        "total_remaining_hours": total_remaining,
        "total_completed_hours": total_completed,
        "tasks": items,
    }


def _build_unassigned(sprint: str) -> dict:
    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] IN ('Task', 'Product Backlog Item') "
        f"AND [System.AssignedTo] = ''"
    )
    results = ado.query_work_items(wiql)
    items = []
    for wi in results:
        f = wi["fields"]
        items.append({
            "id": wi["id"], "type": f.get("System.WorkItemType"),
            "title": f.get("System.Title"), "state": f.get("System.State"),
            "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork"),
        })
    return {"sprint": sprint, "count": len(items), "items": items}


# ══════════════════════════════════════════════════════════════════════
#  DIRECT API ROUTES — no OpenAI cost
# ══════════════════════════════════════════════════════════════════════


# Debug endpoint — shows raw AssignedTo values from TFS to diagnose matching
@app.route("/api/debug-assignees")
def api_debug_assignees():
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Task'"
        )
        tasks = ado.query_work_items(wiql)
        results = []
        for wi in tasks:
            f = wi["fields"]
            raw_assigned = f.get("System.AssignedTo")
            a_name, a_unique, a_id = _get_assignee_info(f)
            matched = _match_member(a_name, a_unique, a_id)
            results.append({
                "id": wi["id"],
                "title": f.get("System.Title"),
                "raw_assigned_to": raw_assigned,
                "raw_type": type(raw_assigned).__name__,
                "parsed_name": a_name,
                "parsed_unique": a_unique,
                "parsed_id": a_id,
                "matched_to": matched,
            })
        matched_count = sum(1 for r in results if r["matched_to"])
        unmatched = [r for r in results if not r["matched_to"] and r["parsed_name"]]
        team = _load_team_data()
        return jsonify({
            "sprint": sprint,
            "total_tasks": len(results),
            "matched": matched_count,
            "unmatched": len(unmatched),
            "team_in_file": [{"displayName": m["displayName"], "uniqueName": m.get("uniqueName", "")} for m in team],
            "unmatched_samples": unmatched[:20],
            "matched_samples": [r for r in results if r["matched_to"]][:10],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    if "session_id" not in session:
        session["session_id"] = os.urandom(16).hex()
    return render_template("index.html", project=AZURE_DEVOPS_PROJECT, team_members=get_team_members())


@app.route("/users")
def users_page():
    return render_template("users.html", project=AZURE_DEVOPS_PROJECT)


@app.route("/api/tfs-users")
def api_tfs_users():
    """Fetch all users from TFS teams."""
    try:
        users = ado.get_team_members()
        return jsonify({"users": users})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/selected-users")
def api_selected_users():
    """Get the saved selected team members (full objects)."""
    return jsonify({"members": _load_team_data()})


@app.route("/api/selected-users", methods=["POST"])
def api_save_selected_users():
    """Save selected team members to local JSON file.
    Expects: {"members": [{"displayName": "...", "uniqueName": "...", "id": "..."}, ...]}
    """
    data = request.get_json()
    members = data.get("members", [])
    _save_team(members)
    return jsonify({"ok": True, "count": len(members)})


@app.route("/api/sprints")
def api_sprints():
    try:
        sprints = ado.get_iterations()
        result = []
        for s in sprints:
            info = {"name": s["name"], "path": s["path"]}
            if "attributes" in s:
                info["start_date"] = s["attributes"].get("startDate")
                info["end_date"] = s["attributes"].get("finishDate")
                info["time_frame"] = s["attributes"].get("timeFrame")
            result.append(info)
        return jsonify({"sprints": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/team-workload")
def api_team_workload():
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(_build_team_workload(sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-summary")
def api_sprint_summary():
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(_build_sprint_summary(sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tasks-by-state")
def api_tasks_by_state():
    sprint = request.args.get("sprint") or _get_current_sprint()
    states = request.args.get("states", "Active,In Progress").split(",")
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(_build_tasks_by_state(sprint, states))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/member-tasks")
def api_member_tasks():
    member = request.args.get("member")
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not member:
        return jsonify({"error": "member parameter required"}), 400
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(_build_member_tasks(member, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/unassigned")
def api_unassigned():
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(_build_unassigned(sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/free-capacity")
def api_free_capacity():
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        data = _build_team_workload(sprint)
        free = [
            {"member": m["member"], "free_hours": m["free_hours"],
             "allocated_hours": m["total_allocated_hours"],
             "capacity_hours": m["capacity_hours"], "task_count": m["task_count"]}
            for m in data["members"]
        ]
        free.sort(key=lambda x: x["free_hours"], reverse=True)
        return jsonify({"sprint": sprint, "members": free})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/team-members")
def api_team_members():
    return jsonify({"team_members": get_team_members(), "count": len(get_team_members())})


@app.route("/api/recent-activity")
def api_recent_activity():
    """Get recent work item changes for the activity feed."""
    hours = int(request.args.get("hours", 24))
    try:
        items = ado.get_recent_activity(hours)
        activities = []
        for wi in items:
            f = wi["fields"]
            changed_by = f.get("System.ChangedBy")
            if isinstance(changed_by, dict):
                changed_by = changed_by.get("displayName", "")
            created_by = f.get("System.CreatedBy")
            if isinstance(created_by, dict):
                created_by = created_by.get("displayName", "")

            changed_date = f.get("System.ChangedDate", "")
            created_date = f.get("System.CreatedDate", "")

            # Determine action: created vs updated
            is_new = changed_date and created_date and changed_date[:16] == created_date[:16]

            activities.append({
                "id": wi["id"],
                "title": f.get("System.Title", ""),
                "type": f.get("System.WorkItemType", ""),
                "state": f.get("System.State", ""),
                "action": "created" if is_new else "updated",
                "changed_by": changed_by or "",
                "changed_date": changed_date,
                "assigned_to": _get_assignee(f),
            })
        return jsonify({"count": len(activities), "activities": activities})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-data")
def api_sprint_data():
    """Single endpoint — fetches ALL work items for a sprint in 2 TFS calls.
    Returns raw items. Frontend processes everything client-side."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        # 1. Get sprint info + iterations (1 TFS call)
        sprints = ado.get_iterations()
        sprint_info = None
        iteration_id = None
        for s in sprints:
            if s["path"] == sprint:
                sprint_info = {
                    "name": s["name"], "path": s["path"],
                    "start_date": s.get("attributes", {}).get("startDate"),
                    "end_date": s.get("attributes", {}).get("finishDate"),
                }
                iteration_id = s.get("id")
                break
        if not sprint_info:
            sprint_name = sprint.rsplit("\\", 1)[-1] if "\\" in sprint else sprint
            sprint_info = {"name": sprint_name, "path": sprint}

        # 2. Get ALL work items in one WIQL query (1 TFS call + batch fetches)
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] IN ('Task', 'Product Backlog Item', 'Bug') "
            f"ORDER BY [System.WorkItemType], [System.AssignedTo]"
        )
        all_items = ado.query_work_items(wiql)

        # 3. Get capacity (1 TFS call)
        capacity_data = []
        team_days_off = []
        if iteration_id:
            try:
                cap_resp = ado.get_capacities(iteration_id)
                capacity_data = cap_resp.get("value", [])
            except Exception:
                pass
            try:
                tdo_resp = ado.get_teamdaysoff(iteration_id)
                team_days_off = tdo_resp.get("daysOff", [])
            except Exception:
                pass

        # 4. Flatten work items to lightweight JSON
        items = []
        for wi in all_items:
            f = wi["fields"]
            a_name, a_unique, a_id = _get_assignee_info(f)
            display_name = a_name
            if not display_name and a_unique:
                lookup = _get_team_lookup()
                display_name = lookup["unique"].get(a_unique.lower(), a_unique.rsplit("\\", 1)[-1])

            changed_by = f.get("System.ChangedBy")
            if isinstance(changed_by, dict):
                changed_by = changed_by.get("displayName", "")

            items.append({
                "id": wi["id"],
                "type": f.get("System.WorkItemType"),
                "title": f.get("System.Title"),
                "state": f.get("System.State"),
                "assigned_to": display_name,
                "assigned_unique": a_unique,
                "assigned_id": a_id,
                "original_estimate": f.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0,
                "remaining_work": f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0,
                "completed_work": f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0,
                "priority": f.get("Microsoft.VSTS.Common.Priority"),
                "severity": f.get("Microsoft.VSTS.Common.Severity"),
                "changed_date": f.get("System.ChangedDate", ""),
                "created_date": f.get("System.CreatedDate", ""),
                "changed_by": changed_by or "",
                "parent_id": _get_parent_id(wi),
            })

        # 5. Capacity per member
        cap_members = []
        for entry in capacity_data:
            tm = entry.get("teamMember", {})
            cap_per_day = sum(a.get("capacityPerDay", 0) for a in entry.get("activities", []))
            days_off_list = entry.get("daysOff", [])
            off_days = 0
            for doff in days_off_list:
                ds_str, de_str = doff.get("start", ""), doff.get("end", "")
                if ds_str and de_str:
                    try:
                        ds = datetime.fromisoformat(ds_str.replace("Z", "+00:00")).date()
                        de = datetime.fromisoformat(de_str.replace("Z", "+00:00")).date()
                        off_days += _count_working_days(ds, de)
                    except (ValueError, TypeError):
                        pass
            # Raw off date ranges for display
            off_dates_raw = []
            for doff in days_off_list:
                ds_str, de_str = doff.get("start", ""), doff.get("end", "")
                if ds_str and de_str:
                    off_dates_raw.append({"start": ds_str[:10], "end": de_str[:10]})

            cap_members.append({
                "displayName": tm.get("displayName", ""),
                "uniqueName": tm.get("uniqueName", ""),
                "capacityPerDay": cap_per_day,
                "daysOff": off_days,
                "daysOffDates": off_dates_raw,
            })

        # Team days off count + raw dates
        team_off = 0
        team_off_dates = []
        for doff in team_days_off:
            ds_str, de_str = doff.get("start", ""), doff.get("end", "")
            if ds_str and de_str:
                try:
                    ds = datetime.fromisoformat(ds_str.replace("Z", "+00:00")).date()
                    de = datetime.fromisoformat(de_str.replace("Z", "+00:00")).date()
                    team_off += _count_working_days(ds, de)
                    team_off_dates.append({"start": ds_str[:10], "end": de_str[:10]})
                except (ValueError, TypeError):
                    pass

        # Day info
        day_info = _get_sprint_day_info(sprint_info)

        # Team members for matching
        team = _load_team_data()

        return jsonify({
            "sprint": sprint_info,
            "day_info": day_info,
            "team": [{"displayName": m["displayName"], "uniqueName": m.get("uniqueName", ""), "id": m.get("id", "")} for m in team],
            "capacity": cap_members,
            "team_days_off": team_off,
            "team_off_dates": team_off_dates,
            "items": items,
            "today": date.today().isoformat(),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/burndown")
def api_burndown():
    """Burndown chart data: remaining work per day across the sprint."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        sprints = ado.get_iterations()
        start_date = end_date = None
        for s in sprints:
            if s["path"] == sprint:
                start_date = s.get("attributes", {}).get("startDate", "")
                end_date = s.get("attributes", {}).get("finishDate", "")
                break
        if not start_date or not end_date:
            return jsonify({"error": "Sprint dates not found"}), 400

        start = datetime.fromisoformat(start_date.replace("Z", "+00:00")).date()
        end = datetime.fromisoformat(end_date.replace("Z", "+00:00")).date()

        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Task'"
        )
        tasks = ado.query_work_items(wiql)

        # Current remaining and original
        total_remaining = 0
        total_original = 0
        for wi in tasks:
            f = wi["fields"]
            total_remaining += f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0
            total_original += f.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0

        total_work = total_original if total_original else total_remaining

        # Build ideal burndown line + actual point
        from datetime import timedelta
        working_days = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                working_days.append(current.isoformat())
            current += timedelta(days=1)

        total_days = len(working_days)
        ideal = []
        for i, day in enumerate(working_days):
            ideal.append({"date": day, "hours": round(total_work - (total_work / max(total_days, 1)) * i, 1)})

        # We can only know the current remaining (TFS doesn't store daily history via API easily)
        today = date.today()
        elapsed = 0
        for d in working_days:
            if d <= today.isoformat():
                elapsed += 1

        return jsonify({
            "sprint": sprint,
            "total_work": total_work,
            "total_remaining": total_remaining,
            "total_completed": total_work - total_remaining,
            "working_days": working_days,
            "ideal": ideal,
            "actual_today": {"day": elapsed, "hours": total_remaining},
            "total_days": total_days,
            "elapsed_days": elapsed,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/standup")
def api_standup():
    """Daily standup view: per member — done yesterday, doing today, blocked/stale."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Task'"
        )
        tasks = ado.query_work_items(wiql)

        today = date.today()
        yesterday = today - __import__('datetime').timedelta(days=1)
        if yesterday.weekday() >= 5:  # skip weekend
            yesterday = today - __import__('datetime').timedelta(days=(today.weekday() - 4) if today.weekday() > 0 else 3)

        stale_threshold = today - __import__('datetime').timedelta(days=2)

        team_standup = {}
        for member in get_team_members():
            team_standup[member] = {"member": member, "yesterday": [], "today": [], "blocked": []}

        for wi in tasks:
            f = wi["fields"]
            a_name, a_unique, a_id = _get_assignee_info(f)
            matched = _match_member(a_name, a_unique, a_id) if a_name else None
            if not matched:
                continue

            state = f.get("System.State", "New")
            changed = f.get("System.ChangedDate", "")
            changed_date = None
            if changed:
                try:
                    changed_date = datetime.fromisoformat(changed.replace("Z", "+00:00")).date()
                except (ValueError, TypeError):
                    pass

            task_info = {
                "id": wi["id"], "title": f.get("System.Title"), "state": state,
                "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0,
                "changed_date": changed,
            }

            m = team_standup[matched]
            if state in ("Closed", "Done", "Resolved") and changed_date and changed_date >= yesterday:
                m["yesterday"].append(task_info)
            elif state in ("Active", "In Progress"):
                m["today"].append(task_info)
                # Stale: active but not updated in 2+ days
                if changed_date and changed_date < stale_threshold:
                    m["blocked"].append(task_info)

        return jsonify({
            "sprint": sprint,
            "date": today.isoformat(),
            "members": [v for v in team_standup.values() if v["yesterday"] or v["today"] or v["blocked"]],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-compare")
def api_sprint_compare():
    """Compare two sprints side by side."""
    sprint1 = request.args.get("sprint1", "")
    sprint2 = request.args.get("sprint2", "")
    if not sprint1 or not sprint2:
        return jsonify({"error": "Both sprint1 and sprint2 required"}), 400
    try:
        s1 = _build_sprint_summary(sprint1)
        s2 = _build_sprint_summary(sprint2)
        return jsonify({"sprint1": s1, "sprint2": s2})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stale-items")
def api_stale_items():
    """Tasks not updated in X days — likely stuck or forgotten."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    days = int(request.args.get("days", 2))
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Task' "
            f"AND [System.State] <> 'Closed' AND [System.State] <> 'Done' AND [System.State] <> 'Resolved' "
            f"AND [System.ChangedDate] < @today - {days} "
            f"ORDER BY [System.ChangedDate] ASC"
        )
        tasks = ado.query_work_items(wiql)
        items = []
        for wi in tasks:
            f = wi["fields"]
            changed = f.get("System.ChangedDate", "")
            days_stale = 0
            if changed:
                try:
                    cd = datetime.fromisoformat(changed.replace("Z", "+00:00")).date()
                    days_stale = (date.today() - cd).days
                except (ValueError, TypeError):
                    pass
            items.append({
                "id": wi["id"], "title": f.get("System.Title"),
                "state": f.get("System.State"), "assigned_to": _get_assignee(f),
                "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0,
                "last_updated": changed, "days_stale": days_stale,
            })
        return jsonify({"sprint": sprint, "threshold_days": days, "count": len(items), "items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pbi-progress")
def api_pbi_progress():
    """PBI progress: each PBI with child task counts and hours."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        pbi_wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Product Backlog Item'"
        )
        pbis = ado.query_work_items(pbi_wiql)

        task_wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Task'"
        )
        tasks = ado.query_work_items(task_wiql)

        # Map tasks to parent PBI
        pbi_tasks = {}
        for wi in tasks:
            parent = _get_parent_id(wi)
            if parent:
                if parent not in pbi_tasks:
                    pbi_tasks[parent] = []
                f = wi["fields"]
                pbi_tasks[parent].append({
                    "id": wi["id"], "title": f.get("System.Title"),
                    "state": f.get("System.State"),
                    "remaining": f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0,
                    "completed": f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0,
                })

        result = []
        for wi in pbis:
            f = wi["fields"]
            pid = wi["id"]
            children = pbi_tasks.get(pid, [])
            total_tasks = len(children)
            done = sum(1 for t in children if t["state"] in ("Closed", "Done", "Resolved"))
            in_prog = sum(1 for t in children if t["state"] in ("Active", "In Progress"))
            total_rem = sum(t["remaining"] for t in children)
            total_comp = sum(t["completed"] for t in children)
            total_work = total_rem + total_comp
            pct = round((total_comp / total_work) * 100, 1) if total_work > 0 else (100 if done == total_tasks and total_tasks > 0 else 0)

            result.append({
                "id": pid, "title": f.get("System.Title"), "state": f.get("System.State"),
                "total_tasks": total_tasks, "done": done, "in_progress": in_prog,
                "todo": total_tasks - done - in_prog,
                "remaining_hours": total_rem, "completed_hours": total_comp,
                "progress_pct": pct, "tasks": children,
            })

        return jsonify({"sprint": sprint, "count": len(result), "pbis": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/velocity")
def api_velocity():
    """Member velocity: avg hours completed per sprint over last N sprints."""
    count = int(request.args.get("sprints", 5))
    try:
        sprints = ado.get_iterations()
        # Get last N sprints (past + current)
        relevant = [s for s in sprints if s.get("attributes", {}).get("timeFrame") in ("past", "current")]
        relevant = relevant[-count:]

        members_velocity = {}
        for member in get_team_members():
            members_velocity[member] = {"member": member, "sprints": [], "total_completed": 0}

        for s in relevant:
            sprint_path = s["path"]
            sprint_name = s["name"]
            wiql = (
                f"SELECT [System.Id] FROM WorkItems "
                f"WHERE [System.IterationPath] = '{sprint_path}' "
                f"AND [System.WorkItemType] = 'Task'"
            )
            tasks = ado.query_work_items(wiql)

            sprint_data = {}
            for member in get_team_members():
                sprint_data[member] = 0

            for wi in tasks:
                f = wi["fields"]
                a_name, a_unique, a_id = _get_assignee_info(f)
                matched = _match_member(a_name, a_unique, a_id) if a_name else None
                if matched:
                    sprint_data[matched] += f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0

            for member, hours in sprint_data.items():
                members_velocity[member]["sprints"].append({"sprint": sprint_name, "completed": hours})
                members_velocity[member]["total_completed"] += hours

        # Calculate averages
        for m in members_velocity.values():
            s_count = len(m["sprints"])
            m["avg_completed"] = round(m["total_completed"] / s_count, 1) if s_count > 0 else 0

        result = [v for v in members_velocity.values() if v["total_completed"] > 0]
        result.sort(key=lambda x: x["avg_completed"], reverse=True)
        return jsonify({"sprint_count": len(relevant), "sprint_names": [s["name"] for s in relevant], "members": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bugs")
def api_bugs():
    """Bug tracker: active bugs, assignees, age."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Bug' "
            f"ORDER BY [System.CreatedDate] ASC"
        )
        bugs = ado.query_work_items(wiql)
        items = []
        active_count = 0
        resolved_count = 0
        for wi in bugs:
            f = wi["fields"]
            state = f.get("System.State", "New")
            created = f.get("System.CreatedDate", "")
            age = 0
            if created:
                try:
                    cd = datetime.fromisoformat(created.replace("Z", "+00:00")).date()
                    age = (date.today() - cd).days
                except (ValueError, TypeError):
                    pass
            if state in ("Closed", "Done", "Resolved"):
                resolved_count += 1
            else:
                active_count += 1
            items.append({
                "id": wi["id"], "title": f.get("System.Title"),
                "state": state, "assigned_to": _get_assignee(f),
                "priority": f.get("Microsoft.VSTS.Common.Priority"),
                "severity": f.get("Microsoft.VSTS.Common.Severity"),
                "created_date": created, "age_days": age,
            })
        return jsonify({
            "sprint": sprint, "total": len(items),
            "active": active_count, "resolved": resolved_count,
            "bugs": items,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-health")
def api_sprint_health():
    """Sprint health score: red/yellow/green based on metrics."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        # Get sprint day info
        sprints = ado.get_iterations()
        sprint_info = {}
        for s in sprints:
            if s["path"] == sprint:
                sprint_info = {
                    "start_date": s.get("attributes", {}).get("startDate"),
                    "end_date": s.get("attributes", {}).get("finishDate"),
                }
                break
        day_info = _get_sprint_day_info(sprint_info)

        # Get tasks
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Task'"
        )
        tasks = ado.query_work_items(wiql)

        total = len(tasks)
        done = in_prog = new_count = 0
        total_remaining = total_completed = 0
        stale = 0
        stale_threshold = date.today() - __import__('datetime').timedelta(days=2)

        for wi in tasks:
            f = wi["fields"]
            state = f.get("System.State", "New")
            if state in ("Closed", "Done", "Resolved"):
                done += 1
            elif state in ("Active", "In Progress"):
                in_prog += 1
            else:
                new_count += 1
            total_remaining += f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0
            total_completed += f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0

            changed = f.get("System.ChangedDate", "")
            if changed and state not in ("Closed", "Done", "Resolved"):
                try:
                    cd = datetime.fromisoformat(changed.replace("Z", "+00:00")).date()
                    if cd < stale_threshold:
                        stale += 1
                except (ValueError, TypeError):
                    pass

        # Scoring (0-100)
        scores = []
        reasons = []

        # 1. Progress vs time (40 pts)
        if day_info["total_days"] > 0 and total > 0:
            time_pct = day_info["elapsed_days"] / day_info["total_days"]
            done_pct = done / total
            progress_score = min(40, round((done_pct / max(time_pct, 0.01)) * 40))
            scores.append(progress_score)
            if done_pct < time_pct * 0.5:
                reasons.append("Tasks completion significantly behind schedule")
            elif done_pct < time_pct:
                reasons.append("Tasks completion slightly behind schedule")
        else:
            scores.append(40)

        # 2. Active work ratio (20 pts)
        if total > 0:
            active_ratio = in_prog / total
            if active_ratio > 0.3:
                scores.append(20)
            elif active_ratio > 0.1:
                scores.append(15)
            else:
                scores.append(5)
                reasons.append("Low active task ratio")
        else:
            scores.append(20)

        # 3. Remaining hours vs days left (20 pts)
        if day_info["remaining_days"] > 0:
            hours_per_day_needed = total_remaining / (day_info["remaining_days"] * 8 * max(len(get_team_members()), 1))
            if hours_per_day_needed <= 1:
                scores.append(20)
            elif hours_per_day_needed <= 1.5:
                scores.append(15)
                reasons.append("Remaining work is high for days left")
            else:
                scores.append(5)
                reasons.append("Remaining work exceeds available capacity")
        else:
            scores.append(10)

        # 4. Stale items (20 pts)
        if total > 0:
            stale_pct = stale / total
            if stale_pct == 0:
                scores.append(20)
            elif stale_pct < 0.1:
                scores.append(15)
            elif stale_pct < 0.2:
                scores.append(10)
                reasons.append(f"{stale} tasks not updated in 2+ days")
            else:
                scores.append(5)
                reasons.append(f"{stale} tasks stale (not updated in 2+ days)")
        else:
            scores.append(20)

        total_score = sum(scores)
        if total_score >= 75:
            status = "green"
            label = "Healthy"
        elif total_score >= 50:
            status = "yellow"
            label = "At Risk"
        else:
            status = "red"
            label = "Critical"

        return jsonify({
            "sprint": sprint,
            "score": total_score,
            "status": status,
            "label": label,
            "reasons": reasons,
            "metrics": {
                "total_tasks": total, "done": done, "in_progress": in_prog, "new": new_count,
                "stale_items": stale, "total_remaining": total_remaining,
                "total_completed": total_completed,
                "elapsed_days": day_info["elapsed_days"], "total_days": day_info["total_days"],
                "remaining_days": day_info["remaining_days"],
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/dashboard")
def api_dashboard():
    """Single endpoint that returns everything needed for the dashboard — one call."""
    sprint = request.args.get("sprint") or _get_current_sprint()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        # Get sprint info
        sprint_name = sprint.rsplit("\\", 1)[-1] if "\\" in sprint else sprint
        sprints = ado.get_iterations()
        sprint_info = {}
        iteration_id = None
        for s in sprints:
            if s["path"] == sprint:
                sprint_info = {
                    "name": s["name"], "path": s["path"],
                    "start_date": s.get("attributes", {}).get("startDate"),
                    "end_date": s.get("attributes", {}).get("finishDate"),
                }
                iteration_id = s.get("id")
                break

        # All tasks in sprint (single query, reused for everything)
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Task'"
        )
        all_tasks = ado.query_work_items(wiql)

        # PBIs
        pbi_wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.IterationPath] = '{sprint}' "
            f"AND [System.WorkItemType] = 'Product Backlog Item'"
        )
        all_pbis = ado.query_work_items(pbi_wiql)

        # Build everything from the same data
        pbi_states = {}
        for wi in all_pbis:
            s = wi["fields"].get("System.State", "New")
            pbi_states[s] = pbi_states.get(s, 0) + 1

        task_states = {}
        total_original = total_remaining = total_completed = 0
        in_progress_tasks = []
        completed_tasks = []
        not_started_tasks = []
        unassigned_tasks = []

        team_data = {}
        for member in get_team_members():
            team_data[member] = {
                "member": member,
                "total_allocated_hours": 0, "remaining_hours": 0,
                "completed_hours": 0, "in_progress_hours": 0,
                "task_count": 0,
                "done_count": 0, "in_progress_count": 0, "new_count": 0,
                "tasks": [],
            }

        for wi in all_tasks:
            f = wi["fields"]
            state = f.get("System.State", "New")
            task_states[state] = task_states.get(state, 0) + 1

            original = f.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0
            remaining = f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0
            completed = f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0
            allocated = original if original else remaining
            total_original += original
            total_remaining += remaining
            total_completed += completed

            a_name, a_unique, a_id = _get_assignee_info(f)
            task_row = {
                "id": wi["id"], "title": f.get("System.Title"), "state": state,
                "assigned_to": a_name,
                "allocated_hours": allocated,
                "remaining_hours": remaining, "completed_hours": completed,
            }

            if state in ("Active", "In Progress"):
                in_progress_tasks.append(task_row)
            elif state in ("Closed", "Done", "Resolved"):
                completed_tasks.append(task_row)
            else:
                not_started_tasks.append(task_row)

            if not a_name:
                unassigned_tasks.append(task_row)

            matched = _match_member(a_name, a_unique, a_id) if a_name else None
            if matched:
                m = team_data[matched]
                m["total_allocated_hours"] += allocated
                m["remaining_hours"] += remaining
                m["completed_hours"] += completed
                m["task_count"] += 1
                m["tasks"].append(task_row)
                if state in ("Closed", "Done", "Resolved"):
                    m["done_count"] += 1
                elif state in ("Active", "In Progress"):
                    m["in_progress_count"] += 1
                    m["in_progress_hours"] += remaining
                else:
                    m["new_count"] += 1

        # Sprint day calculations
        sp = sprint_info or {"name": sprint_name, "path": sprint}
        day_info = _get_sprint_day_info(sp)

        # Fetch TFS capacity per member (includes days off, leaves)
        member_capacity = {}  # uniqueName/displayName → capacity_per_day
        team_days_off = []
        if iteration_id:
            try:
                cap_data = ado.get_capacities(iteration_id)
                for entry in cap_data.get("value", []):
                    tm = entry.get("teamMember", {})
                    unique = tm.get("uniqueName", "")
                    display = tm.get("displayName", "")
                    # Sum capacity from all activities
                    cap_per_day = sum(
                        a.get("capacityPerDay", 0)
                        for a in entry.get("activities", [])
                    )
                    # Count personal days off
                    days_off = entry.get("daysOff", [])
                    off_count = 0
                    for doff in days_off:
                        start_str = doff.get("start", "")
                        end_str = doff.get("end", "")
                        if start_str and end_str:
                            try:
                                ds = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
                                de = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
                                off_count += _count_working_days(ds, de)
                            except (ValueError, TypeError):
                                pass

                    member_capacity[unique.lower()] = {
                        "capacity_per_day": cap_per_day,
                        "days_off": off_count,
                    }
                    member_capacity[display.lower()] = member_capacity[unique.lower()]
            except Exception:
                pass

            try:
                tdo = ado.get_teamdaysoff(iteration_id)
                team_days_off = tdo.get("daysOff", [])
            except Exception:
                pass

        # Count team-wide days off
        team_off_count = 0
        for doff in team_days_off:
            start_str = doff.get("start", "")
            end_str = doff.get("end", "")
            if start_str and end_str:
                try:
                    ds = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
                    de = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
                    team_off_count += _count_working_days(ds, de)
                except (ValueError, TypeError):
                    pass

        # Calculate per-member capacity and progress
        today = date.today()
        for m in team_data.values():
            remaining = m["remaining_hours"]
            member_name = m["member"]

            # Find this member's TFS capacity
            cap_info = (
                member_capacity.get(member_name.lower())
                or next((v for k, v in member_capacity.items()
                         if member_name.lower() in k or k in member_name.lower()), None)
            )

            # Also try matching via uniqueName from team_members.json
            if not cap_info:
                for td in _load_team_data():
                    if td["displayName"] == member_name and td.get("uniqueName"):
                        cap_info = member_capacity.get(td["uniqueName"].lower())
                        break

            if cap_info and cap_info["capacity_per_day"] > 0:
                cpd = cap_info["capacity_per_day"]
                personal_off = cap_info["days_off"]
                effective_days = max(0, day_info["total_days"] - team_off_count - personal_off)
                total_cap = cpd * effective_days

                # Remaining capacity: remaining working days × capacity_per_day
                # (subtract personal off days that haven't passed yet)
                remaining_effective_days = max(0, day_info["remaining_days"])
                # Simple: remaining_cap = remaining_days * capacity_per_day
                remaining_cap = cpd * remaining_effective_days

                m["total_capacity"] = total_cap
                m["remaining_capacity"] = remaining_cap
            else:
                # Fallback: no TFS capacity data
                m["total_capacity"] = 0
                m["remaining_capacity"] = 0

            total_cap = m["total_capacity"]
            remaining_cap = m["remaining_capacity"]

            if remaining_cap > 0 and m["task_count"] > 0:
                # Bar fill = remaining_work / remaining_capacity (like TFS)
                # Full green = fully loaded, empty = no work assigned
                m["progress_pct"] = round(min(100, (remaining / remaining_cap) * 100), 1)
                m["expected_pct"] = round((day_info["elapsed_days"] / day_info["total_days"]) * 100, 1) if day_info["total_days"] > 0 else 0
                # Behind: remaining work > remaining capacity hours
                m["behind"] = remaining > remaining_cap
            elif m["task_count"] > 0 and remaining_cap == 0:
                # Sprint ended or no capacity left
                m["progress_pct"] = 100 if remaining > 0 else 0
                m["expected_pct"] = 100
                m["behind"] = remaining > 0
            else:
                m["progress_pct"] = 0
                m["expected_pct"] = 0
                m["behind"] = False

        return jsonify({
            "sprint": sp,
            "day_info": day_info,
            "summary": {
                "total_pbis": len(all_pbis), "pbi_states": pbi_states,
                "total_tasks": len(all_tasks), "task_states": task_states,
                "total_original": total_original,
                "total_completed": total_completed,
                "total_remaining": total_remaining,
            },
            "in_progress": in_progress_tasks,
            "completed": completed_tasks,
            "not_started": not_started_tasks,
            "unassigned": unassigned_tasks,
            "members": list(team_data.values()),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  AI CHAT — only used for custom/complex questions
# ══════════════════════════════════════════════════════════════════════

TOOLS = [
    {"type": "function", "function": {
        "name": "create_pbi",
        "description": "Create a Product Backlog Item (PBI) in Azure DevOps. Returns the created PBI's ID which MUST be used as parent_id when creating tasks under it.",
        "parameters": {"type": "object", "properties": {
            "title": {"type": "string", "description": "Title of the PBI"},
            "description": {"type": "string", "description": "Detailed description with acceptance criteria in HTML format"},
            "sprint": {"type": "string", "description": "Full iteration path e.g. 'Digital Bank\\Sprint 77'"},
            "assigned_to": {"type": "string", "description": "Display name of assignee (optional)"},
            "priority": {"type": "integer", "enum": [1, 2, 3, 4], "description": "Priority 1=highest (optional)"}
        }, "required": ["title", "description", "sprint"]}
    }},
    {"type": "function", "function": {
        "name": "create_task",
        "description": "Create a Task in Azure DevOps. IMPORTANT: Always set parent_id to link the task under a PBI. If the user asks to create tasks for a PBI, first create the PBI, get its ID from the response, then create each task with that ID as parent_id.",
        "parameters": {"type": "object", "properties": {
            "title": {"type": "string", "description": "Title of the task"},
            "description": {"type": "string", "description": "Task description in HTML format"},
            "sprint": {"type": "string", "description": "Full iteration path"},
            "parent_id": {"type": "integer", "description": "REQUIRED: Work item ID of the parent PBI. Get this from the create_pbi response."},
            "assigned_to": {"type": "string", "description": "Display name of assignee (optional)"},
            "remaining_hours": {"type": "number", "description": "Estimated hours for the task (optional)"}
        }, "required": ["title", "description", "sprint", "parent_id"]}
    }},
    {"type": "function", "function": {"name": "update_status", "description": "Update state of a work item (New, Active, Resolved, Closed).", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}, "state": {"type": "string"}}, "required": ["work_item_id", "state"]}}},
    {"type": "function", "function": {"name": "update_remaining_hours", "description": "Update remaining hours on a task.", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}, "hours": {"type": "number"}}, "required": ["work_item_id", "hours"]}}},
    {"type": "function", "function": {"name": "change_assignee", "description": "Change assignee of a work item.", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}, "assigned_to": {"type": "string"}}, "required": ["work_item_id", "assigned_to"]}}},
    {"type": "function", "function": {"name": "get_work_item", "description": "Retrieve a work item by ID.", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}}, "required": ["work_item_id"]}}},
    {"type": "function", "function": {"name": "list_sprints", "description": "List all sprints.", "parameters": {"type": "object", "properties": {}, "required": []}}},
    {"type": "function", "function": {"name": "query_work_items", "description": "Run a WIQL query.", "parameters": {"type": "object", "properties": {"wiql": {"type": "string"}}, "required": ["wiql"]}}},
    {"type": "function", "function": {"name": "fetch_url", "description": "Fetch and read the content of a URL. Use this when the user shares a link and asks you to read or analyze it.", "parameters": {"type": "object", "properties": {"url": {"type": "string", "description": "The URL to fetch"}}, "required": ["url"]}}},
]

def _build_system_prompt():
    team_list = ", ".join(get_team_members())
    return f"""You are an Azure DevOps assistant for project "{AZURE_DEVOPS_PROJECT}".
Team: {team_list}

CRITICAL RULES:
1. When the user asks to create a PBI with tasks, you MUST follow this exact sequence:
   Step 1: Call create_pbi to create the PBI. Wait for the response to get the PBI ID.
   Step 2: Call create_task for each task, passing the PBI ID as parent_id. Do NOT create tasks without parent_id.
   NEVER create tasks in parallel with the PBI — always create the PBI first, get its ID, then create tasks.

2. When the user says "Sprint X", use the full path: "{AZURE_DEVOPS_PROJECT}\\Sprint X".
3. Always ask for sprint if not provided. Call list_sprints to show options.
4. Write PBI descriptions in HTML with acceptance criteria.
5. Use markdown tables in responses.
6. Always confirm created items by showing work item ID."""


def execute_tool(name: str, args: dict) -> str:
    try:
        if name == "create_pbi":
            r = ado.create_pbi(args["title"], args["description"], args["sprint"], args.get("assigned_to"), args.get("priority"))
            return json.dumps({"success": True, "id": r["id"], "url": r["_links"]["html"]["href"], "title": r["fields"]["System.Title"]})
        elif name == "create_task":
            parent_id = args.get("parent_id")
            r = ado.create_task(args["title"], args["description"], args["sprint"], parent_id, args.get("assigned_to"), args.get("remaining_hours"))
            return json.dumps({"success": True, "id": r["id"], "url": r["_links"]["html"]["href"], "title": r["fields"]["System.Title"], "parent_id": parent_id})
        elif name == "update_status":
            r = ado.update_status(args["work_item_id"], args["state"])
            return json.dumps({"success": True, "id": r["id"], "new_state": r["fields"]["System.State"]})
        elif name == "update_remaining_hours":
            r = ado.update_remaining_hours(args["work_item_id"], args["hours"])
            return json.dumps({"success": True, "id": r["id"], "remaining_hours": r["fields"].get("Microsoft.VSTS.Scheduling.RemainingWork")})
        elif name == "change_assignee":
            r = ado.change_assignee(args["work_item_id"], args["assigned_to"])
            return json.dumps({"success": True, "id": r["id"], "assigned_to": r["fields"].get("System.AssignedTo", {}).get("displayName", args["assigned_to"])})
        elif name == "get_work_item":
            r = ado.get_work_item(args["work_item_id"])
            f = r["fields"]
            return json.dumps({"id": r["id"], "type": f.get("System.WorkItemType"), "title": f.get("System.Title"), "state": f.get("System.State"), "assigned_to": _get_assignee(f), "sprint": f.get("System.IterationPath"), "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork")})
        elif name == "list_sprints":
            sprints = ado.get_iterations()
            return json.dumps({"sprints": [{"name": s["name"], "path": s["path"], "time_frame": s.get("attributes", {}).get("timeFrame")} for s in sprints]})
        elif name == "query_work_items":
            results = ado.query_work_items(args["wiql"])
            return json.dumps({"count": len(results), "items": [{"id": wi["id"], "type": wi["fields"].get("System.WorkItemType"), "title": wi["fields"].get("System.Title"), "state": wi["fields"].get("System.State"), "assigned_to": _get_assignee(wi["fields"])} for wi in results]})
        elif name == "fetch_url":
            import requests as req
            import re
            url = args["url"]
            resp = req.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            content = resp.text
            # Strip HTML to get clean text (removes scripts, styles, tags)
            content = re.sub(r'<script[^>]*>[\s\S]*?</script>', '', content, flags=re.IGNORECASE)
            content = re.sub(r'<style[^>]*>[\s\S]*?</style>', '', content, flags=re.IGNORECASE)
            content = re.sub(r'<[^>]+>', ' ', content)
            content = re.sub(r'\s+', ' ', content).strip()
            # GPT-4o supports ~128K tokens, allow up to 50K chars (~12K tokens)
            if len(content) > 50000:
                content = content[:50000] + "\n\n... (truncated, page too large)"
            return json.dumps({"url": url, "status": resp.status_code, "length": len(content), "content": content})
        return json.dumps({"error": f"Unknown tool: {name}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


conversations = {}


def get_messages(session_id: str) -> list:
    if session_id not in conversations:
        conversations[session_id] = [{"role": "system", "content": _build_system_prompt()}]
    return conversations[session_id]


@app.route("/api/chat", methods=["POST"])
def chat():
    if not ai:
        return jsonify({"error": "OpenAI API key not configured. Set OPENAI_API_KEY in .env"}), 500

    data = request.get_json()
    user_message = data.get("message", "").strip()
    if not user_message:
        return jsonify({"error": "Empty message"}), 400

    session_id = session.get("session_id", os.urandom(16).hex())
    session["session_id"] = session_id
    messages = get_messages(session_id)
    messages.append({"role": "user", "content": user_message})

    tool_calls_log = []
    while True:
        response = ai.chat.completions.create(
            model="gpt-4o", messages=messages, tools=TOOLS, tool_choice="auto",
        )
        msg = response.choices[0].message
        msg_dict = {"role": "assistant", "content": msg.content}
        if msg.tool_calls:
            msg_dict["tool_calls"] = [{"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}} for tc in msg.tool_calls]
        messages.append(msg_dict)

        if not msg.tool_calls:
            break

        for tc in msg.tool_calls:
            result = execute_tool(tc.function.name, json.loads(tc.function.arguments))
            tool_calls_log.append({"tool": tc.function.name, "result": json.loads(result)})
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

    return jsonify({"reply": msg.content or "", "tool_calls": tool_calls_log})


@app.route("/api/reset", methods=["POST"])
def reset():
    sid = session.get("session_id")
    if sid and sid in conversations:
        del conversations[sid]
    return jsonify({"ok": True})


if __name__ == "__main__":
    print("Starting Azure DevOps Agent on http://localhost:5000")
    app.run(debug=True, port=5000)
