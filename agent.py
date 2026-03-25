"""
ArthaFin Agent — powered by OpenAI API.

An interactive agent that creates PBIs, tasks, updates statuses,
remaining hours, and assignees in Azure DevOps / TFS using natural language.
"""

import json
import os
import sys
from openai import OpenAI
from dotenv import load_dotenv
from azure_devops_client import AzureDevOpsClient

load_dotenv()

# ── Configuration ───────────────────────────────────────────────────

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
AZURE_DEVOPS_ORG_URL = os.environ.get("AZURE_DEVOPS_ORG_URL")
AZURE_DEVOPS_PAT = os.environ.get("AZURE_DEVOPS_PAT")
AZURE_DEVOPS_PROJECT = os.environ.get("AZURE_DEVOPS_PROJECT")

if not all([OPENAI_API_KEY, AZURE_DEVOPS_ORG_URL, AZURE_DEVOPS_PAT, AZURE_DEVOPS_PROJECT]):
    print("Error: Missing required environment variables.")
    print("Copy .env.example to .env and fill in your values.")
    sys.exit(1)

client = OpenAI(api_key=OPENAI_API_KEY)
ado = AzureDevOpsClient(AZURE_DEVOPS_ORG_URL, AZURE_DEVOPS_PAT, AZURE_DEVOPS_PROJECT)

# ── Tool Definitions (OpenAI function calling format) ───────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "create_pbi",
            "description": (
                "Create a Product Backlog Item (PBI) in Azure DevOps. "
                "Use this when the user asks to create a PBI, user story, or backlog item. "
                "You MUST ask the user for the sprint/iteration path if they haven't provided it."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Title of the PBI"
                    },
                    "description": {
                        "type": "string",
                        "description": "Detailed description/acceptance criteria in HTML format"
                    },
                    "sprint": {
                        "type": "string",
                        "description": "Full iteration path, e.g. 'ProjectName\\Sprint 1'"
                    },
                    "assigned_to": {
                        "type": "string",
                        "description": "Display name or email of the assignee (optional)"
                    },
                    "priority": {
                        "type": "integer",
                        "description": "Priority 1-4 where 1 is highest (optional)",
                        "enum": [1, 2, 3, 4]
                    }
                },
                "required": ["title", "description", "sprint"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_task",
            "description": (
                "Create a Task in Azure DevOps, optionally linked to a parent PBI. "
                "Use this when the user asks to create a task or sub-task. "
                "You MUST ask the user for the sprint/iteration path if they haven't provided it."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Title of the task"
                    },
                    "description": {
                        "type": "string",
                        "description": "Description of the task in HTML format"
                    },
                    "sprint": {
                        "type": "string",
                        "description": "Full iteration path, e.g. 'ProjectName\\Sprint 1'"
                    },
                    "parent_id": {
                        "type": "integer",
                        "description": "Work item ID of the parent PBI to link this task under (optional)"
                    },
                    "assigned_to": {
                        "type": "string",
                        "description": "Display name or email of the assignee (optional)"
                    },
                    "remaining_hours": {
                        "type": "number",
                        "description": "Estimated remaining hours for the task (optional)"
                    }
                },
                "required": ["title", "description", "sprint"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_status",
            "description": (
                "Update the state/status of a work item (PBI or Task). "
                "Common states: New, Active, Resolved, Closed, Removed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "work_item_id": {
                        "type": "integer",
                        "description": "The ID of the work item to update"
                    },
                    "state": {
                        "type": "string",
                        "description": "New state for the work item (e.g. New, Active, Resolved, Closed)"
                    }
                },
                "required": ["work_item_id", "state"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_remaining_hours",
            "description": "Update the remaining hours on a task.",
            "parameters": {
                "type": "object",
                "properties": {
                    "work_item_id": {
                        "type": "integer",
                        "description": "The ID of the task to update"
                    },
                    "hours": {
                        "type": "number",
                        "description": "New remaining hours value"
                    }
                },
                "required": ["work_item_id", "hours"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "change_assignee",
            "description": "Change the person assigned to a work item.",
            "parameters": {
                "type": "object",
                "properties": {
                    "work_item_id": {
                        "type": "integer",
                        "description": "The ID of the work item to reassign"
                    },
                    "assigned_to": {
                        "type": "string",
                        "description": "Display name or email of the new assignee"
                    }
                },
                "required": ["work_item_id", "assigned_to"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_work_item",
            "description": "Retrieve details of a specific work item by its ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "work_item_id": {
                        "type": "integer",
                        "description": "The ID of the work item to retrieve"
                    }
                },
                "required": ["work_item_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_sprints",
            "description": "List all available sprints/iterations for the project. Use this to help the user pick a sprint.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_work_items",
            "description": (
                "Search for work items using a WIQL query. "
                "Use this to find existing PBIs, tasks, or check what's in a sprint."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "wiql": {
                        "type": "string",
                        "description": "A WIQL (Work Item Query Language) query string"
                    }
                },
                "required": ["wiql"]
            }
        }
    },
]

# ── Tool Execution ──────────────────────────────────────────────────


def execute_tool(name: str, input_data: dict) -> str:
    """Execute a tool and return the result as a JSON string."""
    try:
        if name == "create_pbi":
            result = ado.create_pbi(
                title=input_data["title"],
                description=input_data["description"],
                sprint=input_data["sprint"],
                assigned_to=input_data.get("assigned_to"),
                priority=input_data.get("priority"),
            )
            return json.dumps({
                "success": True,
                "id": result["id"],
                "url": result["_links"]["html"]["href"],
                "title": result["fields"]["System.Title"],
            })

        elif name == "create_task":
            result = ado.create_task(
                title=input_data["title"],
                description=input_data["description"],
                sprint=input_data["sprint"],
                parent_id=input_data.get("parent_id"),
                assigned_to=input_data.get("assigned_to"),
                remaining_hours=input_data.get("remaining_hours"),
            )
            return json.dumps({
                "success": True,
                "id": result["id"],
                "url": result["_links"]["html"]["href"],
                "title": result["fields"]["System.Title"],
            })

        elif name == "update_status":
            result = ado.update_status(input_data["work_item_id"], input_data["state"])
            return json.dumps({
                "success": True,
                "id": result["id"],
                "new_state": result["fields"]["System.State"],
            })

        elif name == "update_remaining_hours":
            result = ado.update_remaining_hours(input_data["work_item_id"], input_data["hours"])
            return json.dumps({
                "success": True,
                "id": result["id"],
                "remaining_hours": result["fields"].get("Microsoft.VSTS.Scheduling.RemainingWork"),
            })

        elif name == "change_assignee":
            result = ado.change_assignee(input_data["work_item_id"], input_data["assigned_to"])
            return json.dumps({
                "success": True,
                "id": result["id"],
                "assigned_to": result["fields"].get("System.AssignedTo", {}).get("displayName", input_data["assigned_to"]),
            })

        elif name == "get_work_item":
            result = ado.get_work_item(input_data["work_item_id"])
            fields = result["fields"]
            return json.dumps({
                "id": result["id"],
                "type": fields.get("System.WorkItemType"),
                "title": fields.get("System.Title"),
                "state": fields.get("System.State"),
                "assigned_to": fields.get("System.AssignedTo", {}).get("displayName") if isinstance(fields.get("System.AssignedTo"), dict) else fields.get("System.AssignedTo"),
                "sprint": fields.get("System.IterationPath"),
                "description": fields.get("System.Description", ""),
                "remaining_hours": fields.get("Microsoft.VSTS.Scheduling.RemainingWork"),
                "priority": fields.get("Microsoft.VSTS.Common.Priority"),
            })

        elif name == "list_sprints":
            sprints = ado.get_iterations()
            sprint_list = []
            for s in sprints:
                sprint_info = {"name": s["name"], "path": s["path"]}
                if "attributes" in s:
                    sprint_info["start_date"] = s["attributes"].get("startDate")
                    sprint_info["end_date"] = s["attributes"].get("finishDate")
                    sprint_info["time_frame"] = s["attributes"].get("timeFrame")
                sprint_list.append(sprint_info)
            return json.dumps({"sprints": sprint_list})

        elif name == "query_work_items":
            results = ado.query_work_items(input_data["wiql"])
            items = []
            for wi in results:
                fields = wi["fields"]
                items.append({
                    "id": wi["id"],
                    "type": fields.get("System.WorkItemType"),
                    "title": fields.get("System.Title"),
                    "state": fields.get("System.State"),
                    "assigned_to": fields.get("System.AssignedTo", {}).get("displayName") if isinstance(fields.get("System.AssignedTo"), dict) else fields.get("System.AssignedTo"),
                    "sprint": fields.get("System.IterationPath"),
                })
            return json.dumps({"count": len(items), "items": items})

        else:
            return json.dumps({"error": f"Unknown tool: {name}"})

    except Exception as e:
        return json.dumps({"error": str(e)})


# ── System Prompt ───────────────────────────────────────────────────

SYSTEM_PROMPT = f"""You are an Azure DevOps assistant that helps manage work items in the project "{AZURE_DEVOPS_PROJECT}".

You can:
- Create Product Backlog Items (PBIs) with detailed descriptions
- Create Tasks (optionally linked to a parent PBI)
- Update work item status (New, Active, Resolved, Closed)
- Update remaining hours on tasks
- Change assignees on work items
- Look up existing work items
- List available sprints

IMPORTANT RULES:
1. When creating a PBI or Task, you MUST have a sprint/iteration path. If the user hasn't provided one, ASK them for it. You can also call list_sprints to show them available options.
2. When creating PBIs, write good structured descriptions in HTML format based on the user's requirements. Include acceptance criteria when appropriate.
3. When the user describes requirements, break them down into a clear PBI title and description. You can suggest creating sub-tasks too.
4. When the user says "Sprint X", convert it to the full iteration path format: "{AZURE_DEVOPS_PROJECT}\\Sprint X" (unless they give a full path).
5. Always confirm what you've created/updated by showing the work item ID and a link.
6. If a user wants to create multiple tasks under a PBI, create the PBI first, then create each task linked to that PBI.

Be conversational and helpful. If the user's request is unclear, ask clarifying questions."""

# ── Agent Loop ──────────────────────────────────────────────────────


def run_agent():
    """Run the interactive agent loop."""
    print("=" * 60)
    print("  ArthaFin Agent")
    print("  Powered by OpenAI GPT-4o")
    print("=" * 60)
    print()
    print("I can help you create PBIs, tasks, update statuses,")
    print("change assignees, and more in Azure DevOps.")
    print("Type 'quit' or 'exit' to stop.")
    print()

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit"):
            print("Goodbye!")
            break

        messages.append({"role": "user", "content": user_input})

        # Agentic loop: keep going until the model stops calling tools
        while True:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
            )

            assistant_message = response.choices[0].message
            messages.append(assistant_message)

            # Print text response if any
            if assistant_message.content:
                print(f"\nAgent: {assistant_message.content}")

            # If no tool calls, we're done with this turn
            if not assistant_message.tool_calls:
                break

            # Execute each tool call and append results
            for tool_call in assistant_message.tool_calls:
                fn_name = tool_call.function.name
                fn_args = json.loads(tool_call.function.arguments)

                print(f"\n  [Calling {fn_name}...]")
                result = execute_tool(fn_name, fn_args)
                result_data = json.loads(result)

                if result_data.get("error"):
                    print(f"  [Error: {result_data['error']}]")
                elif result_data.get("success"):
                    print(f"  [Done - Work Item #{result_data.get('id', '?')}]")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": result,
                })

        print()


if __name__ == "__main__":
    run_agent()
