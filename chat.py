"""AI Chat — OpenAI-powered agent for custom TFS operations."""

import json
import os
import re
import requests as req
from datetime import datetime

from openai import OpenAI

from config import OPENAI_API_KEY, AZURE_DEVOPS_PROJECT, CHAT_HISTORY_FILE
from helpers import get_team_members, get_assignee


# ── OpenAI Client ──

ai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ── Tool Definitions ──

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
        "description": "Create a Task in Azure DevOps. IMPORTANT: Always set parent_id to link the task under a PBI.",
        "parameters": {"type": "object", "properties": {
            "title": {"type": "string", "description": "Title of the task"},
            "description": {"type": "string", "description": "Task description in HTML format"},
            "sprint": {"type": "string", "description": "Full iteration path"},
            "parent_id": {"type": "integer", "description": "REQUIRED: Work item ID of the parent PBI."},
            "assigned_to": {"type": "string", "description": "Display name of assignee (optional)"},
            "remaining_hours": {"type": "number", "description": "Estimated hours for the task (optional)"}
        }, "required": ["title", "description", "sprint", "parent_id"]}
    }},
    {"type": "function", "function": {"name": "update_status", "description": "Update state of a work item.", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}, "state": {"type": "string"}}, "required": ["work_item_id", "state"]}}},
    {"type": "function", "function": {"name": "update_remaining_hours", "description": "Update remaining hours on a task.", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}, "hours": {"type": "number"}}, "required": ["work_item_id", "hours"]}}},
    {"type": "function", "function": {"name": "change_assignee", "description": "Change assignee of a work item.", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}, "assigned_to": {"type": "string"}}, "required": ["work_item_id", "assigned_to"]}}},
    {"type": "function", "function": {"name": "get_work_item", "description": "Retrieve a work item by ID.", "parameters": {"type": "object", "properties": {"work_item_id": {"type": "integer"}}, "required": ["work_item_id"]}}},
    {"type": "function", "function": {"name": "list_sprints", "description": "List all sprints.", "parameters": {"type": "object", "properties": {}, "required": []}}},
    {"type": "function", "function": {"name": "query_work_items", "description": "Run a WIQL query.", "parameters": {"type": "object", "properties": {"wiql": {"type": "string"}}, "required": ["wiql"]}}},
    {"type": "function", "function": {"name": "fetch_url", "description": "Fetch and read the content of a URL.", "parameters": {"type": "object", "properties": {"url": {"type": "string", "description": "The URL to fetch"}}, "required": ["url"]}}},
]


def build_system_prompt():
    team_list = ", ".join(get_team_members())
    return f"""You are an Azure DevOps assistant for project "{AZURE_DEVOPS_PROJECT}".
Team: {team_list}

CRITICAL RULES:
1. When creating a PBI with tasks: create PBI first, get its ID, then create tasks with that ID as parent_id.
2. Sprint format: "{AZURE_DEVOPS_PROJECT}\\Sprint X".
3. Always ask for sprint if not provided.
4. Write PBI descriptions in HTML with acceptance criteria.
5. Use markdown tables in responses.
6. Always confirm created items by showing work item ID."""


def execute_tool(ado, name: str, args: dict) -> str:
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
            return json.dumps({"id": r["id"], "type": f.get("System.WorkItemType"), "title": f.get("System.Title"), "state": f.get("System.State"), "assigned_to": get_assignee(f), "sprint": f.get("System.IterationPath"), "remaining_hours": f.get("Microsoft.VSTS.Scheduling.RemainingWork")})
        elif name == "list_sprints":
            sprints = ado.get_iterations()
            return json.dumps({"sprints": [{"name": s["name"], "path": s["path"], "time_frame": s.get("attributes", {}).get("timeFrame")} for s in sprints]})
        elif name == "query_work_items":
            results = ado.query_work_items(args["wiql"])
            return json.dumps({"count": len(results), "items": [{"id": wi["id"], "type": wi["fields"].get("System.WorkItemType"), "title": wi["fields"].get("System.Title"), "state": wi["fields"].get("System.State"), "assigned_to": get_assignee(wi["fields"])} for wi in results]})
        elif name == "fetch_url":
            url = args["url"]
            resp = req.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            content = resp.text
            content = re.sub(r'<script[^>]*>[\s\S]*?</script>', '', content, flags=re.IGNORECASE)
            content = re.sub(r'<style[^>]*>[\s\S]*?</style>', '', content, flags=re.IGNORECASE)
            content = re.sub(r'<[^>]+>', ' ', content)
            content = re.sub(r'\s+', ' ', content).strip()
            if len(content) > 50000:
                content = content[:50000] + "\n\n... (truncated, page too large)"
            return json.dumps({"url": url, "status": resp.status_code, "length": len(content), "content": content})
        return json.dumps({"error": f"Unknown tool: {name}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ── Chat History ──

conversations = {}


def _load_chat_history():
    if os.path.exists(CHAT_HISTORY_FILE):
        try:
            with open(CHAT_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"chats": {}}
    return {"chats": {}}


def _save_chat_history(data):
    with open(CHAT_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_chat_session(chat_id, title, ui_messages, api_messages):
    history = _load_chat_history()
    history["chats"][chat_id] = {
        "id": chat_id,
        "title": title,
        "updated_at": datetime.now().isoformat(),
        "ui_messages": ui_messages,
        "api_messages": [m for m in api_messages if m.get("role") != "system"],
    }
    _save_chat_history(history)


def get_messages(session_id: str) -> list:
    if session_id not in conversations:
        conversations[session_id] = [{"role": "system", "content": build_system_prompt()}]
    return conversations[session_id]


def process_chat(ado, user_message: str, chat_id: str) -> dict:
    """Process a chat message through OpenAI with tool calling."""
    if not ai:
        return {"error": "OpenAI API key not configured. Set OPENAI_API_KEY in .env"}

    if not chat_id:
        chat_id = os.urandom(16).hex()

    messages = get_messages(chat_id)
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
            result = execute_tool(ado, tc.function.name, json.loads(tc.function.arguments))
            tool_calls_log.append({"tool": tc.function.name, "result": json.loads(result)})
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

    return {"reply": msg.content or "", "tool_calls": tool_calls_log, "chat_id": chat_id}


def list_chats() -> list:
    history = _load_chat_history()
    chats = []
    for cid, c in history.get("chats", {}).items():
        chats.append({"id": c["id"], "title": c.get("title", "Untitled"), "updated_at": c.get("updated_at", "")})
    chats.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return chats


def load_chat(chat_id: str) -> dict | None:
    history = _load_chat_history()
    c = history.get("chats", {}).get(chat_id)
    if not c:
        return None
    conversations[chat_id] = [{"role": "system", "content": build_system_prompt()}] + c.get("api_messages", [])
    return {"id": c["id"], "title": c.get("title", ""), "ui_messages": c.get("ui_messages", [])}


def delete_chat(chat_id: str):
    history = _load_chat_history()
    if chat_id in history.get("chats", {}):
        del history["chats"][chat_id]
        _save_chat_history(history)
    if chat_id in conversations:
        del conversations[chat_id]


def rename_chat(chat_id: str, title: str):
    history = _load_chat_history()
    if chat_id in history.get("chats", {}):
        history["chats"][chat_id]["title"] = title
        _save_chat_history(history)
