"""Analytics builders — all TFS data processing functions."""

from datetime import date, datetime, timedelta

from config import HOURS_PER_DAY
from helpers import (
    get_team_members, load_team_data, get_team_lookup,
    get_assignee_info, get_assignee, match_member, find_unique_name,
    get_parent_id, count_working_days, get_sprint_day_info,
    parse_hours_from_title,
)


def build_team_workload(ado, sprint: str, sprint_days: int = 10) -> dict:
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
        a_name, a_unique, a_id = get_assignee_info(fields)
        if not a_name:
            continue
        matched = match_member(a_name, a_unique, a_id)
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


def build_sprint_summary(ado, sprint: str) -> dict:
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


def build_tasks_by_state(ado, sprint: str, states: list[str]) -> dict:
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
            "assigned_to": get_assignee(f),
            "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0,
            "completed_hours": f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0,
        })
    return {"sprint": sprint, "states": states, "count": len(items), "items": items}


def build_member_tasks(ado, member: str, sprint: str) -> dict:
    unique = find_unique_name(member)
    filter_clause = f"[System.AssignedTo] = '{unique}'" if unique else f"[System.AssignedTo] = '{member}'"
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
            "parent_id": get_parent_id(wi),
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


def build_member_cross_project_tasks(ado, member: str, sprint: str) -> dict:
    """Fetch tasks for a member across ALL projects, not just the configured one."""
    unique = find_unique_name(member)
    filter_clause = f"[System.AssignedTo] = '{unique}'" if unique else f"[System.AssignedTo] = '{member}'"

    # Query without project filter to search across all projects
    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.WorkItemType] = 'Task' "
        f"AND {filter_clause} "
        f"AND [System.State] <> 'Closed' AND [System.State] <> 'Removed' "
        f"ORDER BY [System.ChangedDate] DESC"
    )
    all_tasks = ado.query_work_items_cross_project(wiql)

    # Also get sprint-specific tasks from current project
    sprint_wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task' "
        f"AND {filter_clause} "
        f"ORDER BY [System.State]"
    )
    sprint_tasks = ado.query_work_items(sprint_wiql)

    # Merge: sprint tasks first, then cross-project (deduplicated)
    seen_ids = set()
    items = []
    total_remaining = total_completed = total_allocated = 0

    def process_task(wi, source):
        nonlocal total_remaining, total_completed, total_allocated
        if wi["id"] in seen_ids:
            return
        seen_ids.add(wi["id"])
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
            "parent_id": get_parent_id(wi),
            "project": f.get("System.TeamProject", ""),
            "iteration": f.get("System.IterationPath", ""),
            "source": source,
        })
        total_remaining += remaining
        total_completed += completed
        total_allocated += allocated

    for wi in sprint_tasks:
        process_task(wi, "current_sprint")
    for wi in all_tasks:
        process_task(wi, "cross_project")

    return {
        "member": member, "sprint": sprint, "task_count": len(items),
        "total_allocated_hours": total_allocated,
        "total_remaining_hours": total_remaining,
        "total_completed_hours": total_completed,
        "cross_project": True,
        "tasks": items,
    }


def build_unassigned(ado, sprint: str) -> dict:
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


def _fetch_one_sprint(ado, project: str, sprint: str, lookup: dict):
    """Fetch work items, capacity, and day info for a single project+sprint combo."""
    sprints = ado.get_iterations(project=project)
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

    # ALL work items in one WIQL query
    # Escape single quotes in sprint path for WIQL
    safe_sprint = sprint.replace("'", "''")
    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{safe_sprint}' "
        f"AND [System.WorkItemType] IN ('Task', 'Product Backlog Item', 'Bug') "
        f"ORDER BY [System.WorkItemType], [System.AssignedTo]"
    )
    try:
        all_items = ado.query_work_items(wiql, project=project)
    except Exception:
        all_items = []

    # Capacity
    capacity_data = []
    team_days_off_raw = []
    if iteration_id:
        try:
            cap_resp = ado.get_capacities(iteration_id, project=project)
            capacity_data = cap_resp.get("value", [])
        except Exception:
            pass
        try:
            tdo_resp = ado.get_teamdaysoff(iteration_id, project=project)
            team_days_off_raw = tdo_resp.get("daysOff", [])
        except Exception:
            pass

    # Flatten work items
    items = []
    for wi in all_items:
        f = wi["fields"]
        a_name, a_unique, a_id = get_assignee_info(f)
        display_name = a_name
        if not display_name and a_unique:
            display_name = lookup["unique"].get(a_unique.lower(), a_unique.rsplit("\\", 1)[-1])

        changed_by = f.get("System.ChangedBy")
        if isinstance(changed_by, dict):
            changed_by = changed_by.get("displayName", "")

        title = f.get("System.Title", "")
        tfs_original = f.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0
        tfs_completed = f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0
        remaining = f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0

        parsed_hours = parse_hours_from_title(title)
        original_estimate = tfs_original if tfs_original else parsed_hours

        if tfs_completed:
            completed_work = tfs_completed
        elif original_estimate and remaining < original_estimate:
            completed_work = round(original_estimate - remaining, 2)
        else:
            completed_work = 0

        items.append({
            "id": wi["id"],
            "type": f.get("System.WorkItemType"),
            "title": title,
            "state": f.get("System.State"),
            "assigned_to": display_name,
            "assigned_unique": a_unique,
            "assigned_id": a_id,
            "original_estimate": original_estimate,
            "remaining_work": remaining,
            "completed_work": completed_work,
            "priority": f.get("Microsoft.VSTS.Common.Priority"),
            "severity": f.get("Microsoft.VSTS.Common.Severity"),
            "changed_date": f.get("System.ChangedDate", ""),
            "created_date": f.get("System.CreatedDate", ""),
            "changed_by": changed_by or "",
            "parent_id": get_parent_id(wi),
            "project": f.get("System.TeamProject", ""),
        })

    # Capacity per member
    cap_members = []
    for entry in capacity_data:
        tm = entry.get("teamMember", {})
        cap_per_day = sum(a.get("capacityPerDay", 0) for a in entry.get("activities", []))
        days_off_list = entry.get("daysOff", [])
        off_days = 0
        off_dates_raw = []
        for doff in days_off_list:
            ds_str, de_str = doff.get("start", ""), doff.get("end", "")
            if ds_str and de_str:
                try:
                    ds = datetime.fromisoformat(ds_str.replace("Z", "+00:00")).date()
                    de = datetime.fromisoformat(de_str.replace("Z", "+00:00")).date()
                    off_days += count_working_days(ds, de)
                except (ValueError, TypeError):
                    pass
                off_dates_raw.append({"start": ds_str[:10], "end": de_str[:10]})

        cap_members.append({
            "displayName": tm.get("displayName", ""),
            "uniqueName": tm.get("uniqueName", ""),
            "capacityPerDay": cap_per_day,
            "daysOff": off_days,
            "daysOffDates": off_dates_raw,
        })

    # Team days off
    team_off = 0
    team_off_dates = []
    for doff in team_days_off_raw:
        ds_str, de_str = doff.get("start", ""), doff.get("end", "")
        if ds_str and de_str:
            try:
                ds = datetime.fromisoformat(ds_str.replace("Z", "+00:00")).date()
                de = datetime.fromisoformat(de_str.replace("Z", "+00:00")).date()
                team_off += count_working_days(ds, de)
                team_off_dates.append({"start": ds_str[:10], "end": de_str[:10]})
            except (ValueError, TypeError):
                pass

    day_info = get_sprint_day_info(sprint_info)

    return {
        "sprint_info": sprint_info,
        "day_info": day_info,
        "items": items,
        "capacity": cap_members,
        "team_off": team_off,
        "team_off_dates": team_off_dates,
    }


def build_sprint_data(ado, sprint: str, team_override: list[dict] | None = None,
                      project_sprint_pairs: list[dict] | None = None) -> dict:
    """Fetch ALL work items for one or more project+sprint combos.
    Returns merged items. Frontend processes everything client-side.

    project_sprint_pairs: [{"project": "X", "sprint": "X\\Sprint 1"}, ...]
    If not provided, falls back to single sprint query on the default project.
    """
    lookup = get_team_lookup()

    # Build list of (project, sprint) pairs to query
    pairs = []
    if project_sprint_pairs:
        for ps in project_sprint_pairs:
            pairs.append((ps["project"], ps["sprint"]))
    else:
        # Legacy single-project mode: extract project from sprint path or use default
        proj = sprint.split("\\")[0] if "\\" in sprint else ado.project
        pairs.append((proj, sprint))

    # Fetch data for each pair and merge
    all_items = []
    all_cap_members = []
    primary_sprint_info = None
    primary_day_info = None
    total_team_off = 0
    all_team_off_dates = []
    seen_ids = set()
    seen_cap = set()
    selected_sprints = []

    for proj, sp in pairs:
        result = _fetch_one_sprint(ado, proj, sp, lookup)
        sprint_label = sp.rsplit("\\", 1)[-1] if "\\" in sp else sp
        selected_sprints.append({"project": proj, "sprint": sp, "sprint_name": sprint_label})

        # Use first pair as primary for day_info and sprint_info
        if primary_sprint_info is None:
            primary_sprint_info = result["sprint_info"]
            primary_day_info = result["day_info"]

        # Merge items (deduplicate by ID)
        for item in result["items"]:
            if item["id"] not in seen_ids:
                seen_ids.add(item["id"])
                all_items.append(item)

        # Merge capacity (deduplicate by uniqueName)
        for cap in result["capacity"]:
            cap_key = cap["uniqueName"].lower()
            if cap_key and cap_key not in seen_cap:
                seen_cap.add(cap_key)
                all_cap_members.append(cap)

        # Accumulate team off (use max across projects)
        if result["team_off"] > total_team_off:
            total_team_off = result["team_off"]
            all_team_off_dates = result["team_off_dates"]

    if not primary_sprint_info:
        sprint_name = sprint.rsplit("\\", 1)[-1] if "\\" in sprint else sprint
        primary_sprint_info = {"name": sprint_name, "path": sprint}
    if not primary_day_info:
        primary_day_info = get_sprint_day_info(primary_sprint_info)

    team = load_team_data(team_override)

    return {
        "sprint": primary_sprint_info,
        "day_info": primary_day_info,
        "team": [{"displayName": m["displayName"], "uniqueName": m.get("uniqueName", ""), "id": m.get("id", "")} for m in team],
        "capacity": all_cap_members,
        "team_days_off": total_team_off,
        "team_off_dates": all_team_off_dates,
        "items": all_items,
        "today": date.today().isoformat(),
        "selected_sprints": selected_sprints,
    }


def build_burndown(ado, sprint: str) -> dict:
    sprints = ado.get_iterations()
    start_date = end_date = None
    for s in sprints:
        if s["path"] == sprint:
            start_date = s.get("attributes", {}).get("startDate", "")
            end_date = s.get("attributes", {}).get("finishDate", "")
            break
    if not start_date or not end_date:
        return {"error": "Sprint dates not found"}

    start = datetime.fromisoformat(start_date.replace("Z", "+00:00")).date()
    end = datetime.fromisoformat(end_date.replace("Z", "+00:00")).date()

    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task'"
    )
    tasks = ado.query_work_items(wiql)

    total_remaining = 0
    total_original = 0
    for wi in tasks:
        f = wi["fields"]
        total_remaining += f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0
        total_original += f.get("Microsoft.VSTS.Scheduling.OriginalEstimate", 0) or 0

    total_work = total_original if total_original else total_remaining

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

    today = date.today()
    elapsed = sum(1 for d in working_days if d <= today.isoformat())

    return {
        "sprint": sprint,
        "total_work": total_work,
        "total_remaining": total_remaining,
        "total_completed": total_work - total_remaining,
        "working_days": working_days,
        "ideal": ideal,
        "actual_today": {"day": elapsed, "hours": total_remaining},
        "total_days": total_days,
        "elapsed_days": elapsed,
    }


def build_standup(ado, sprint: str) -> dict:
    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task'"
    )
    tasks = ado.query_work_items(wiql)

    today = date.today()
    yesterday = today - timedelta(days=1)
    if yesterday.weekday() >= 5:
        yesterday = today - timedelta(days=(today.weekday() - 4) if today.weekday() > 0 else 3)

    stale_threshold = today - timedelta(days=2)

    team_standup = {}
    for member in get_team_members():
        team_standup[member] = {"member": member, "yesterday": [], "today": [], "blocked": []}

    for wi in tasks:
        f = wi["fields"]
        a_name, a_unique, a_id = get_assignee_info(f)
        matched = match_member(a_name, a_unique, a_id) if a_name else None
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
            if changed_date and changed_date < stale_threshold:
                m["blocked"].append(task_info)

    return {
        "sprint": sprint,
        "date": today.isoformat(),
        "members": [v for v in team_standup.values() if v["yesterday"] or v["today"] or v["blocked"]],
    }


def build_sprint_compare(ado, sprint1: str, sprint2: str) -> dict:
    return {"sprint1": build_sprint_summary(ado, sprint1), "sprint2": build_sprint_summary(ado, sprint2)}


def build_stale_items(ado, sprint: str, days: int = 2) -> dict:
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
            "state": f.get("System.State"), "assigned_to": get_assignee(f),
            "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork", 0) or 0,
            "last_updated": changed, "days_stale": days_stale,
        })
    return {"sprint": sprint, "threshold_days": days, "count": len(items), "items": items}


def build_pbi_progress(ado, sprint: str) -> dict:
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

    pbi_tasks = {}
    for wi in tasks:
        parent = get_parent_id(wi)
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

    return {"sprint": sprint, "count": len(result), "pbis": result}


def build_velocity(ado, count: int = 5) -> dict:
    sprints = ado.get_iterations()
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

        sprint_data = {member: 0 for member in get_team_members()}

        for wi in tasks:
            f = wi["fields"]
            a_name, a_unique, a_id = get_assignee_info(f)
            matched = match_member(a_name, a_unique, a_id) if a_name else None
            if matched:
                sprint_data[matched] += f.get("Microsoft.VSTS.Scheduling.CompletedWork", 0) or 0

        for member, hours in sprint_data.items():
            members_velocity[member]["sprints"].append({"sprint": sprint_name, "completed": hours})
            members_velocity[member]["total_completed"] += hours

    for m in members_velocity.values():
        s_count = len(m["sprints"])
        m["avg_completed"] = round(m["total_completed"] / s_count, 1) if s_count > 0 else 0

    result = [v for v in members_velocity.values() if v["total_completed"] > 0]
    result.sort(key=lambda x: x["avg_completed"], reverse=True)
    return {"sprint_count": len(relevant), "sprint_names": [s["name"] for s in relevant], "members": result}


def build_bugs(ado, sprint: str) -> dict:
    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Bug' "
        f"ORDER BY [System.CreatedDate] ASC"
    )
    bugs = ado.query_work_items(wiql)
    items = []
    active_count = resolved_count = 0
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
            "state": state, "assigned_to": get_assignee(f),
            "priority": f.get("Microsoft.VSTS.Common.Priority"),
            "severity": f.get("Microsoft.VSTS.Common.Severity"),
            "created_date": created, "age_days": age,
        })
    return {"sprint": sprint, "total": len(items), "active": active_count, "resolved": resolved_count, "bugs": items}


def build_sprint_health(ado, sprint: str) -> dict:
    sprints = ado.get_iterations()
    sprint_info = {}
    for s in sprints:
        if s["path"] == sprint:
            sprint_info = {
                "start_date": s.get("attributes", {}).get("startDate"),
                "end_date": s.get("attributes", {}).get("finishDate"),
            }
            break
    day_info = get_sprint_day_info(sprint_info)

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
    stale_threshold = date.today() - timedelta(days=2)

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

    scores = []
    reasons = []

    # Progress vs time (40 pts)
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

    # Active work ratio (20 pts)
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

    # Remaining hours vs days left (20 pts)
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

    # Stale items (20 pts)
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
        status, label = "green", "Healthy"
    elif total_score >= 50:
        status, label = "yellow", "At Risk"
    else:
        status, label = "red", "Critical"

    return {
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
    }


def build_dashboard(ado, sprint: str) -> dict:
    """Returns everything needed for the dashboard — one call."""
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

    wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Task'"
    )
    all_tasks = ado.query_work_items(wiql)

    pbi_wiql = (
        f"SELECT [System.Id] FROM WorkItems "
        f"WHERE [System.IterationPath] = '{sprint}' "
        f"AND [System.WorkItemType] = 'Product Backlog Item'"
    )
    all_pbis = ado.query_work_items(pbi_wiql)

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

        a_name, a_unique, a_id = get_assignee_info(f)
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

        matched = match_member(a_name, a_unique, a_id) if a_name else None
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

    sp = sprint_info or {"name": sprint_name, "path": sprint}
    day_info = get_sprint_day_info(sp)

    # TFS capacity per member
    member_capacity = {}
    team_days_off_list = []
    if iteration_id:
        try:
            cap_data = ado.get_capacities(iteration_id)
            for entry in cap_data.get("value", []):
                tm = entry.get("teamMember", {})
                unique = tm.get("uniqueName", "")
                display = tm.get("displayName", "")
                cap_per_day = sum(a.get("capacityPerDay", 0) for a in entry.get("activities", []))
                days_off = entry.get("daysOff", [])
                off_count = 0
                for doff in days_off:
                    start_str = doff.get("start", "")
                    end_str = doff.get("end", "")
                    if start_str and end_str:
                        try:
                            ds = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
                            de = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
                            off_count += count_working_days(ds, de)
                        except (ValueError, TypeError):
                            pass
                member_capacity[unique.lower()] = {"capacity_per_day": cap_per_day, "days_off": off_count}
                member_capacity[display.lower()] = member_capacity[unique.lower()]
        except Exception:
            pass

        try:
            tdo = ado.get_teamdaysoff(iteration_id)
            team_days_off_list = tdo.get("daysOff", [])
        except Exception:
            pass

    team_off_count = 0
    for doff in team_days_off_list:
        start_str = doff.get("start", "")
        end_str = doff.get("end", "")
        if start_str and end_str:
            try:
                ds = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
                de = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
                team_off_count += count_working_days(ds, de)
            except (ValueError, TypeError):
                pass

    # Per-member capacity and progress
    for m in team_data.values():
        remaining = m["remaining_hours"]
        member_name = m["member"]

        cap_info = (
            member_capacity.get(member_name.lower())
            or next((v for k, v in member_capacity.items()
                     if member_name.lower() in k or k in member_name.lower()), None)
        )

        if not cap_info:
            for td in load_team_data():
                if td["displayName"] == member_name and td.get("uniqueName"):
                    cap_info = member_capacity.get(td["uniqueName"].lower())
                    break

        if cap_info and cap_info["capacity_per_day"] > 0:
            cpd = cap_info["capacity_per_day"]
            personal_off = cap_info["days_off"]
            effective_days = max(0, day_info["total_days"] - team_off_count - personal_off)
            total_cap = cpd * effective_days
            remaining_effective_days = max(0, day_info["remaining_days"])
            remaining_cap = cpd * remaining_effective_days
            m["total_capacity"] = total_cap
            m["remaining_capacity"] = remaining_cap
        else:
            m["total_capacity"] = 0
            m["remaining_capacity"] = 0

        total_cap = m["total_capacity"]
        remaining_cap = m["remaining_capacity"]

        if remaining_cap > 0 and m["task_count"] > 0:
            m["progress_pct"] = round(min(100, (remaining / remaining_cap) * 100), 1)
            m["expected_pct"] = round((day_info["elapsed_days"] / day_info["total_days"]) * 100, 1) if day_info["total_days"] > 0 else 0
            m["behind"] = remaining > remaining_cap
        elif m["task_count"] > 0 and remaining_cap == 0:
            m["progress_pct"] = 100 if remaining > 0 else 0
            m["expected_pct"] = 100
            m["behind"] = remaining > 0
        else:
            m["progress_pct"] = 0
            m["expected_pct"] = 0
            m["behind"] = False

    return {
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
    }
