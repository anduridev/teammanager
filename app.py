"""ArthaFin — Flask web app.
Thin routing layer. Business logic lives in analytics.py, chat.py, helpers.py.
"""

import os
from datetime import date
from flask import Flask, render_template, request, jsonify, session, redirect, url_for

from config import (
    AZURE_DEVOPS_ORG_URL, AZURE_DEVOPS_PAT, AZURE_DEVOPS_PROJECT,
    FLASK_SECRET_KEY,
)
from azure_devops_client import AzureDevOpsClient
from helpers import (
    get_team_members, load_team_data, save_team,
    get_current_sprint, get_assignee_info, get_assignee, match_member,
)
from analytics import (
    build_team_workload, build_sprint_summary, build_tasks_by_state,
    build_member_tasks, build_unassigned, build_sprint_data,
    build_burndown, build_standup, build_sprint_compare,
    build_stale_items, build_pbi_progress, build_velocity,
    build_bugs, build_sprint_health, build_dashboard,
    build_member_cross_project_tasks,
)
from chat import (
    process_chat, save_chat_session, list_chats, load_chat,
    delete_chat, rename_chat, get_messages, conversations,
)
from auth import (
    login_required, load_app_config, save_app_config,
    get_current_user, get_user_role, is_superadmin, is_manager,
    get_manager_team, save_manager_team,
    get_manager_projects, save_manager_projects,
    get_all_manager_team_members,
)

# ── App Setup ──

ado = AzureDevOpsClient(AZURE_DEVOPS_ORG_URL, AZURE_DEVOPS_PAT, AZURE_DEVOPS_PROJECT)
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY


@app.before_request
def _begin_cache():
    ado._cache.begin()


@app.after_request
def _end_cache(response):
    ado._cache.end()
    return response


# ── Helper ──

def _sprint_or_current():
    return request.args.get("sprint") or get_current_sprint(ado)


def _my_team():
    """Get the current logged-in user's team members."""
    user = get_current_user()
    team = get_manager_team(user)
    if team:
        return team
    # Fallback to legacy team_members.json
    return load_team_data()


# ══════════════════════════════════════════════════════════════════════
#  Auth — Login / Logout
# ══════════════════════════════════════════════════════════════════════


@app.route("/login")
def login_page():
    if get_current_user():
        return redirect(url_for("index"))
    return render_template("login.html", project=AZURE_DEVOPS_PROJECT)


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json()
    display_name = data.get("displayName", "").strip()
    unique_name = data.get("uniqueName", "").strip()
    user_id = data.get("id", "").strip()
    if not display_name:
        return jsonify({"error": "No user selected"}), 400
    session["user"] = {
        "displayName": display_name,
        "uniqueName": unique_name,
        "id": user_id,
    }
    session["session_id"] = os.urandom(16).hex()
    return jsonify({"ok": True, "role": get_user_role()})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/api/me")
def api_me():
    """Return current user info and role."""
    user = get_current_user()
    if not user:
        return jsonify({"logged_in": False}), 401
    return jsonify({
        "logged_in": True,
        "user": user,
        "role": get_user_role(),
    })


# ══════════════════════════════════════════════════════════════════════
#  Pages (protected)
# ══════════════════════════════════════════════════════════════════════


@app.route("/")
@login_required
def index():
    if "session_id" not in session:
        session["session_id"] = os.urandom(16).hex()
    user = get_current_user()
    role = get_user_role()
    team = _my_team()
    return render_template(
        "index.html",
        project=AZURE_DEVOPS_PROJECT,
        team_members=get_team_members(team),
        user=user,
        role=role,
    )


@app.route("/users")
@login_required
def users_page():
    return render_template("users.html", project=AZURE_DEVOPS_PROJECT)


@app.route("/admin")
@login_required
def admin_page():
    if not is_superadmin():
        return redirect(url_for("index"))
    config = load_app_config()
    return render_template(
        "admin.html",
        project=AZURE_DEVOPS_PROJECT,
        superadmin=config.get("superadmin", ""),
    )


# ══════════════════════════════════════════════════════════════════════
#  API — Admin (superadmin only)
# ══════════════════════════════════════════════════════════════════════


@app.route("/api/admin/managers")
@login_required
def api_admin_get_managers():
    if not is_superadmin():
        return jsonify({"error": "Forbidden"}), 403
    config = load_app_config()
    return jsonify({"managers": config.get("managers", [])})


@app.route("/api/admin/managers", methods=["POST"])
@login_required
def api_admin_add_manager():
    if not is_superadmin():
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json()
    unique_name = data.get("uniqueName", "").strip()
    display_name = data.get("displayName", "").strip()
    user_id = data.get("id", "").strip()
    if not unique_name and not display_name:
        return jsonify({"error": "No user specified"}), 400

    config = load_app_config()
    managers = config.get("managers", [])
    # Check duplicate
    for m in managers:
        if m.get("uniqueName", "").lower() == unique_name.lower():
            return jsonify({"error": "Already a manager"}), 400
    managers.append({
        "uniqueName": unique_name,
        "displayName": display_name,
        "id": user_id,
    })
    config["managers"] = managers
    save_app_config(config)
    return jsonify({"ok": True, "managers": managers})


@app.route("/api/admin/managers", methods=["DELETE"])
@login_required
def api_admin_remove_manager():
    if not is_superadmin():
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json()
    unique_name = data.get("uniqueName", "").strip()
    config = load_app_config()
    managers = config.get("managers", [])
    managers = [m for m in managers if m.get("uniqueName", "").lower() != unique_name.lower()]
    config["managers"] = managers
    save_app_config(config)
    return jsonify({"ok": True, "managers": managers})


# ── Project Management ──


@app.route("/api/my-projects")
@login_required
def api_my_projects():
    """Get the projects this manager is tracking."""
    projects = get_manager_projects()
    return jsonify({"projects": projects})


@app.route("/api/my-projects", methods=["POST"])
@login_required
def api_save_my_projects():
    """Save the projects this manager wants to track."""
    user = get_current_user()
    if not (is_superadmin() or is_manager()):
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json()
    projects = data.get("projects", [])
    save_manager_projects(user, projects)
    return jsonify({"ok": True, "projects": projects})


@app.route("/api/tfs-projects")
@login_required
def api_tfs_projects():
    """List all TFS projects."""
    try:
        url = f"{AZURE_DEVOPS_ORG_URL}/_apis/projects"
        import requests as req
        import base64
        encoded_pat = base64.b64encode(f":{AZURE_DEVOPS_PAT}".encode()).decode()
        headers = {"Authorization": f"Basic {encoded_pat}", "Content-Type": "application/json"}
        resp = req.get(url, headers=headers, params={"api-version": "5.0"})
        resp.raise_for_status()
        projects = [{"name": p["name"], "id": p.get("id", "")} for p in resp.json().get("value", [])]
        projects.sort(key=lambda x: x["name"].lower())
        return jsonify({"projects": projects})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/teams-overview")
@login_required
def api_admin_teams_overview():
    """Superadmin view: who has which team members."""
    if not is_superadmin():
        return jsonify({"error": "Forbidden"}), 403
    teams = get_all_manager_team_members()
    result = []
    for mgr_unique, members in teams.items():
        result.append({
            "manager": mgr_unique,
            "member_count": len(members),
            "members": [m.get("displayName", "") for m in members],
        })
    return jsonify({"teams": result})


@app.route("/api/search-users")
@login_required
def api_search_users():
    """Search ALL TFS users by name via IdentityPicker API. Fast."""
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"users": []})
    try:
        users = ado.search_identities(q)
        return jsonify({"users": users})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — TFS Data (no AI cost)
# ══════════════════════════════════════════════════════════════════════


@app.route("/api/tfs-users")
def api_tfs_users():
    """Public — needed by login page before auth."""
    try:
        return jsonify({"users": ado.get_team_members()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/selected-users")
@login_required
def api_selected_users():
    return jsonify({"members": _my_team()})


@app.route("/api/selected-users", methods=["POST"])
@login_required
def api_save_selected_users():
    user = get_current_user()
    data = request.get_json()
    members = data.get("members", [])
    if is_superadmin() or is_manager():
        save_manager_team(user, members)
    else:
        save_team(members)  # legacy fallback
    return jsonify({"ok": True, "count": len(members)})


@app.route("/api/sprints")
@login_required
def api_sprints():
    project = request.args.get("project")
    try:
        sprints = ado.get_iterations(project=project)
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


@app.route("/api/project-sprints")
@login_required
def api_project_sprints():
    """Get sprints for a specific TFS project."""
    project = request.args.get("project", "").strip()
    if not project:
        return jsonify({"error": "project parameter required"}), 400
    try:
        sprints = ado.get_iterations(project=project)
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
@login_required
def api_team_workload():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_team_workload(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-summary")
@login_required
def api_sprint_summary():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_sprint_summary(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tasks-by-state")
@login_required
def api_tasks_by_state():
    sprint = _sprint_or_current()
    states = request.args.get("states", "Active,In Progress").split(",")
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_tasks_by_state(ado, sprint, states))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/member-tasks")
@login_required
def api_member_tasks():
    member = request.args.get("member")
    sprint = _sprint_or_current()
    cross_project = request.args.get("cross_project", "false").lower() == "true"
    if not member:
        return jsonify({"error": "member parameter required"}), 400
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        if cross_project:
            return jsonify(build_member_cross_project_tasks(ado, member, sprint))
        return jsonify(build_member_tasks(ado, member, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/unassigned")
@login_required
def api_unassigned():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_unassigned(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/free-capacity")
@login_required
def api_free_capacity():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        data = build_team_workload(ado, sprint)
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
@login_required
def api_team_members():
    team = _my_team()
    names = get_team_members(team)
    return jsonify({"team_members": names, "count": len(names)})


@app.route("/api/recent-activity")
@login_required
def api_recent_activity():
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
            is_new = changed_date and created_date and changed_date[:16] == created_date[:16]

            activities.append({
                "id": wi["id"],
                "title": f.get("System.Title", ""),
                "type": f.get("System.WorkItemType", ""),
                "state": f.get("System.State", ""),
                "action": "created" if is_new else "updated",
                "changed_by": changed_by or "",
                "changed_date": changed_date,
                "assigned_to": get_assignee(f),
            })
        return jsonify({"count": len(activities), "activities": activities})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-data")
@login_required
def api_sprint_data():
    # Support multi-project+sprint via JSON query params
    # Format: projects=[{"project":"X","sprint":"X\\Sprint 1"},...]
    import json as _json
    projects_param = request.args.get("projects")
    project_sprint_pairs = None
    if projects_param:
        try:
            project_sprint_pairs = _json.loads(projects_param)
            if not isinstance(project_sprint_pairs, list) or not project_sprint_pairs:
                project_sprint_pairs = None
        except (ValueError, TypeError):
            project_sprint_pairs = None

    if project_sprint_pairs:
        sprint = ""  # not needed when multi-project is active
    else:
        sprint = _sprint_or_current()
        if not sprint:
            return jsonify({"error": "No sprint found. Please select projects & sprints."}), 400
    try:
        return jsonify(build_sprint_data(
            ado, sprint,
            team_override=_my_team(),
            project_sprint_pairs=project_sprint_pairs,
        ))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/burndown")
@login_required
def api_burndown():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        result = build_burndown(ado, sprint)
        if "error" in result:
            return jsonify(result), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/standup")
@login_required
def api_standup():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_standup(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-compare")
@login_required
def api_sprint_compare():
    sprint1 = request.args.get("sprint1", "")
    sprint2 = request.args.get("sprint2", "")
    if not sprint1 or not sprint2:
        return jsonify({"error": "Both sprint1 and sprint2 required"}), 400
    try:
        return jsonify(build_sprint_compare(ado, sprint1, sprint2))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stale-items")
@login_required
def api_stale_items():
    sprint = _sprint_or_current()
    days = int(request.args.get("days", 2))
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_stale_items(ado, sprint, days))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pbi-progress")
@login_required
def api_pbi_progress():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_pbi_progress(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/velocity")
@login_required
def api_velocity():
    count = int(request.args.get("sprints", 5))
    try:
        return jsonify(build_velocity(ado, count))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/bugs")
@login_required
def api_bugs():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_bugs(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sprint-health")
@login_required
def api_sprint_health():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_sprint_health(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/dashboard")
@login_required
def api_dashboard():
    sprint = _sprint_or_current()
    if not sprint:
        return jsonify({"error": "No sprint found"}), 400
    try:
        return jsonify(build_dashboard(ado, sprint))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Debug ──

@app.route("/api/debug-assignees")
@login_required
def api_debug_assignees():
    sprint = _sprint_or_current()
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
            a_name, a_unique, a_id = get_assignee_info(f)
            matched = match_member(a_name, a_unique, a_id)
            results.append({
                "id": wi["id"], "title": f.get("System.Title"),
                "raw_assigned_to": raw_assigned, "raw_type": type(raw_assigned).__name__,
                "parsed_name": a_name, "parsed_unique": a_unique, "parsed_id": a_id,
                "matched_to": matched,
            })
        matched_count = sum(1 for r in results if r["matched_to"])
        unmatched = [r for r in results if not r["matched_to"] and r["parsed_name"]]
        team = load_team_data()
        return jsonify({
            "sprint": sprint, "total_tasks": len(results),
            "matched": matched_count, "unmatched": len(unmatched),
            "team_in_file": [{"displayName": m["displayName"], "uniqueName": m.get("uniqueName", "")} for m in team],
            "unmatched_samples": unmatched[:20],
            "matched_samples": [r for r in results if r["matched_to"]][:10],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
#  API — AI Chat
# ══════════════════════════════════════════════════════════════════════


@app.route("/api/chat", methods=["POST"])
@login_required
def chat():
    data = request.get_json()
    user_message = data.get("message", "").strip()
    chat_id = data.get("chat_id", "")
    if not user_message:
        return jsonify({"error": "Empty message"}), 400
    if not chat_id:
        chat_id = os.urandom(16).hex()
    session["session_id"] = chat_id
    result = process_chat(ado, user_message, chat_id)
    if "error" in result and "reply" not in result:
        return jsonify(result), 500
    return jsonify(result)


@app.route("/api/chat/save", methods=["POST"])
@login_required
def chat_save():
    data = request.get_json()
    chat_id = data.get("chat_id", "")
    title = data.get("title", "Untitled Chat")
    ui_messages = data.get("ui_messages", [])
    if not chat_id:
        return jsonify({"error": "No chat_id"}), 400
    api_messages = conversations.get(chat_id, [])
    save_chat_session(chat_id, title, ui_messages, api_messages)
    return jsonify({"ok": True})


@app.route("/api/chat/list")
@login_required
def chat_list():
    return jsonify(list_chats())


@app.route("/api/chat/load/<chat_id>")
@login_required
def chat_load(chat_id):
    c = load_chat(chat_id)
    if not c:
        return jsonify({"error": "Chat not found"}), 404
    return jsonify(c)


@app.route("/api/chat/delete/<chat_id>", methods=["DELETE"])
@login_required
def chat_delete(chat_id):
    delete_chat(chat_id)
    return jsonify({"ok": True})


@app.route("/api/chat/rename", methods=["POST"])
@login_required
def chat_rename():
    data = request.get_json()
    chat_id = data.get("chat_id", "")
    title = data.get("title", "").strip()
    if not chat_id or not title:
        return jsonify({"error": "Missing chat_id or title"}), 400
    rename_chat(chat_id, title)
    return jsonify({"ok": True})


@app.route("/api/reset", methods=["POST"])
@login_required
def reset():
    sid = session.get("session_id")
    if sid and sid in conversations:
        del conversations[sid]
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Starting ArthaFin on http://localhost:5000")
    app.run(debug=True, port=5000)
