"""Azure DevOps REST API client for managing work items."""

import base64
import json
import time
import requests


class _RequestCache:
    """Per-request cache. Call begin() at start of a request, end() when done.
    Within a request, repeated calls for the same data are served from memory.
    Between requests, cache is empty — always fresh."""

    def __init__(self):
        self._store = {}
        self._active = False

    def begin(self):
        """Start a new request scope."""
        self._store.clear()
        self._active = True

    def end(self):
        """End request scope — discard all cached data."""
        self._store.clear()
        self._active = False

    def get(self, key: str):
        if self._active:
            return self._store.get(key)
        return None

    def set(self, key: str, val):
        if self._active:
            self._store[key] = val

    def clear(self):
        self._store.clear()


class AzureDevOpsClient:
    """Client for Azure DevOps Server / TFS REST API."""

    def __init__(self, org_url: str, pat: str, project: str):
        self.org_url = org_url.rstrip("/")
        self.project = project
        self.api_version = "5.0"
        self._cache = _RequestCache()
        encoded_pat = base64.b64encode(f":{pat}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {encoded_pat}",
            "Content-Type": "application/json-patch+json",
        }
        self._json_headers = {
            "Authorization": f"Basic {encoded_pat}",
            "Content-Type": "application/json",
        }

    def _work_items_url(self):
        return f"{self.org_url}/{self.project}/_apis/wit/workitems"

    def _api_params(self, extra=None):
        params = {"api-version": self.api_version}
        if extra:
            params.update(extra)
        return params

    def clear_cache(self):
        """Clear all cached data. Call after write operations."""
        self._cache.clear()

    # ── Create Work Items ───────────────────────────────────────────

    def create_pbi(self, title: str, description: str, sprint: str,
                   assigned_to: str = None, priority: int = None) -> dict:
        """Create a Product Backlog Item."""
        self.clear_cache()
        url = f"{self._work_items_url()}/$Product Backlog Item"
        ops = [
            {"op": "add", "path": "/fields/System.Title", "value": title},
            {"op": "add", "path": "/fields/System.Description", "value": description},
            {"op": "add", "path": "/fields/System.IterationPath", "value": sprint},
        ]
        if assigned_to:
            ops.append({"op": "add", "path": "/fields/System.AssignedTo", "value": assigned_to})
        if priority:
            ops.append({"op": "add", "path": "/fields/Microsoft.VSTS.Common.Priority", "value": priority})

        resp = requests.post(url, headers=self.headers, params=self._api_params(), json=ops)
        resp.raise_for_status()
        return resp.json()

    def create_task(self, title: str, description: str, sprint: str,
                    parent_id: int = None, assigned_to: str = None,
                    remaining_hours: float = None) -> dict:
        """Create a Task, optionally linked to a parent PBI."""
        self.clear_cache()
        url = f"{self._work_items_url()}/$Task"
        ops = [
            {"op": "add", "path": "/fields/System.Title", "value": title},
            {"op": "add", "path": "/fields/System.Description", "value": description},
            {"op": "add", "path": "/fields/System.IterationPath", "value": sprint},
        ]
        if assigned_to:
            ops.append({"op": "add", "path": "/fields/System.AssignedTo", "value": assigned_to})
        if remaining_hours is not None:
            ops.append({"op": "add", "path": "/fields/Microsoft.VSTS.Scheduling.RemainingWork", "value": remaining_hours})
        if parent_id:
            ops.append({
                "op": "add",
                "path": "/relations/-",
                "value": {
                    "rel": "System.LinkTypes.Hierarchy-Reverse",
                    "url": f"{self.org_url}/_apis/wit/workItems/{parent_id}",
                },
            })

        resp = requests.post(url, headers=self.headers, params=self._api_params(), json=ops)
        resp.raise_for_status()
        return resp.json()

    # ── Update Work Items ───────────────────────────────────────────

    def update_status(self, work_item_id: int, state: str) -> dict:
        """Update the state of a work item (e.g. New, Active, Closed)."""
        self.clear_cache()
        url = f"{self._work_items_url()}/{work_item_id}"
        ops = [{"op": "replace", "path": "/fields/System.State", "value": state}]
        resp = requests.patch(url, headers=self.headers, params=self._api_params(), json=ops)
        resp.raise_for_status()
        return resp.json()

    def update_remaining_hours(self, work_item_id: int, hours: float) -> dict:
        """Update remaining hours on a task."""
        self.clear_cache()
        url = f"{self._work_items_url()}/{work_item_id}"
        ops = [{"op": "replace", "path": "/fields/Microsoft.VSTS.Scheduling.RemainingWork", "value": hours}]
        resp = requests.patch(url, headers=self.headers, params=self._api_params(), json=ops)
        resp.raise_for_status()
        return resp.json()

    def change_assignee(self, work_item_id: int, assigned_to: str) -> dict:
        """Change the assignee of a work item."""
        self.clear_cache()
        url = f"{self._work_items_url()}/{work_item_id}"
        ops = [{"op": "replace", "path": "/fields/System.AssignedTo", "value": assigned_to}]
        resp = requests.patch(url, headers=self.headers, params=self._api_params(), json=ops)
        resp.raise_for_status()
        return resp.json()

    # ── Query Work Items ────────────────────────────────────────────

    def get_work_item(self, work_item_id: int) -> dict:
        """Get a work item by ID."""
        url = f"{self._work_items_url()}/{work_item_id}"
        resp = requests.get(url, headers=self._json_headers, params=self._api_params({"$expand": "relations"}))
        resp.raise_for_status()
        return resp.json()

    def get_iterations(self, project: str = None) -> list:
        """List all iterations/sprints for a project (defaults to self.project)."""
        proj = project or self.project
        cache_key = f"iterations_{proj}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        url = f"{self.org_url}/{proj}/_apis/work/teamsettings/iterations"
        resp = requests.get(url, headers=self._json_headers, params=self._api_params())
        resp.raise_for_status()
        result = resp.json().get("value", [])
        self._cache.set(cache_key, result)
        return result

    def get_recent_activity(self, hours: int = 24, top: int = 100) -> list:
        """Get recently updated work items (created/changed in last N hours), max `top` records."""
        key = f"activity_{hours}_{top}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        url = f"{self.org_url}/{self.project}/_apis/wit/wiql"
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.TeamProject] = '{self.project}' "
            f"AND [System.ChangedDate] >= @today - {hours / 24:.0f} "
            f"AND [System.WorkItemType] IN ('Task', 'Product Backlog Item', 'Bug') "
            f"ORDER BY [System.ChangedDate] DESC"
        )
        resp = requests.post(
            url, headers=self._json_headers,
            params=self._api_params({"$top": str(top)}),
            json={"query": wiql},
        )
        resp.raise_for_status()
        work_items = resp.json().get("workItems", [])[:top]
        if not work_items:
            return []

        ids = ",".join(str(wi["id"]) for wi in work_items)
        details_url = f"{self.org_url}/_apis/wit/workitems"
        details_resp = requests.get(
            details_url, headers=self._json_headers,
            params=self._api_params({"ids": ids}),
        )
        details_resp.raise_for_status()
        result = details_resp.json().get("value", [])
        self._cache.set(key, result)
        return result

    def get_work_item_updates(self, work_item_id: int, top: int = 5) -> list:
        """Get update history for a work item."""
        url = f"{self._work_items_url()}/{work_item_id}/updates"
        resp = requests.get(url, headers=self._json_headers, params=self._api_params({"$top": str(top)}))
        resp.raise_for_status()
        return resp.json().get("value", [])

    def get_capacities(self, iteration_id: str, project: str = None) -> dict:
        """Get team capacity for a sprint iteration, including per-member capacity,
        days off, and team days off. Returns raw TFS response."""
        proj = project or self.project
        key = f"cap_{proj}_{iteration_id}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        url = f"{self.org_url}/{proj}/_apis/work/teamsettings/iterations/{iteration_id}/capacities"
        resp = requests.get(url, headers=self._json_headers, params=self._api_params())
        resp.raise_for_status()
        result = resp.json()
        self._cache.set(key, result)
        return result

    def get_teamdaysoff(self, iteration_id: str, project: str = None) -> dict:
        """Get team days off for a sprint iteration."""
        proj = project or self.project
        key = f"tdo_{proj}_{iteration_id}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        url = f"{self.org_url}/{proj}/_apis/work/teamsettings/iterations/{iteration_id}/teamdaysoff"
        resp = requests.get(url, headers=self._json_headers, params=self._api_params())
        resp.raise_for_status()
        result = resp.json()
        self._cache.set(key, result)
        return result

    def search_identities(self, query: str) -> list:
        """Search ALL TFS users via IdentityPicker API. Fast, works on TFS 2018+."""
        results = []
        seen = set()
        if not query or len(query) < 2:
            return results
        try:
            url = f"{self.org_url}/_apis/IdentityPicker/Identities"
            resp = requests.post(url, headers=self._json_headers, params={
                "api-version": "5.0-preview.1",
            }, json={
                "query": query,
                "identityTypes": ["user"],
                "operationScopes": ["ims", "source"],
                "properties": ["DisplayName", "SamAccountName", "Active"],
                "options": {"MinResults": 5, "MaxResults": 50},
            }, timeout=10)
            resp.raise_for_status()
            identity_results = resp.json().get("results", [])
            identities = identity_results[0].get("identities", []) if identity_results else []
            for identity in identities:
                if not identity.get("active"):
                    continue
                name = identity.get("displayName", "")
                account = identity.get("samAccountName", "")
                uid = identity.get("localId", "")
                unique = f"TFS\\{account}" if account else ""
                if not name or name.startswith("["):
                    continue
                key = (account or name).lower()
                if key not in seen:
                    seen.add(key)
                    results.append({
                        "displayName": name,
                        "uniqueName": unique,
                        "id": uid,
                    })
        except Exception:
            pass
        results.sort(key=lambda x: x["displayName"].lower())
        return results

    def get_team_members(self) -> list:
        """Get TFS users from project teams + work item assignees."""
        all_members = []
        seen = set()

        # 1. Project team members
        team_urls = [
            f"{self.org_url}/_apis/projects/{self.project}/teams",
            f"{self.org_url}/{self.project}/_apis/projects/{self.project}/teams",
        ]
        teams = []
        for url in team_urls:
            try:
                resp = requests.get(url, headers=self._json_headers,
                                    params=self._api_params(), timeout=10)
                resp.raise_for_status()
                teams = resp.json().get("value", [])
                break
            except Exception:
                continue

        for team in teams:
            team_id = team.get("id", "")
            member_urls = [
                f"{self.org_url}/_apis/projects/{self.project}/teams/{team_id}/members",
                f"{self.org_url}/{self.project}/_apis/projects/{self.project}/teams/{team_id}/members",
            ]
            for murl in member_urls:
                try:
                    mr = requests.get(murl, headers=self._json_headers,
                                      params=self._api_params(), timeout=10)
                    mr.raise_for_status()
                    for m in mr.json().get("value", []):
                        identity = m.get("identity", {})
                        name = identity.get("displayName", "")
                        uid = identity.get("uniqueName", "") or identity.get("id", "")
                        if name and name.lower() not in seen:
                            seen.add(name.lower())
                            all_members.append({
                                "displayName": name,
                                "uniqueName": uid,
                                "id": identity.get("id", ""),
                            })
                    break
                except Exception:
                    continue

        all_members.sort(key=lambda x: x["displayName"].lower())
        return all_members

    def query_work_items_cross_project(self, wiql: str) -> list:
        """Run a WIQL query across ALL projects (uses collection-level endpoint)."""
        key = f"wiql_xp_{hash(wiql)}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # Use the collection-level WIQL endpoint (no project in URL)
        url = f"{self.org_url}/_apis/wit/wiql"
        resp = requests.post(
            url,
            headers=self._json_headers,
            params=self._api_params(),
            json={"query": wiql},
        )
        resp.raise_for_status()
        work_items = resp.json().get("workItems", [])
        if not work_items:
            self._cache.set(key, [])
            return []

        # Fetch in batches of 200
        all_details = []
        details_url = f"{self.org_url}/_apis/wit/workitems"
        for i in range(0, len(work_items), 200):
            batch = work_items[i:i + 200]
            ids = ",".join(str(wi["id"]) for wi in batch)
            details_resp = requests.get(
                details_url,
                headers=self._json_headers,
                params=self._api_params({"ids": ids, "$expand": "relations"}),
            )
            details_resp.raise_for_status()
            all_details.extend(details_resp.json().get("value", []))

        self._cache.set(key, all_details)
        return all_details

    def query_work_items(self, wiql: str, project: str = None) -> list:
        """Run a WIQL query and return matching work items.
        Handles pagination — TFS batch API accepts max 200 IDs per call.
        Results are cached for 60s."""
        proj = project or self.project
        key = f"wiql_{proj}_{hash(wiql)}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        url = f"{self.org_url}/{proj}/_apis/wit/wiql"
        resp = requests.post(
            url,
            headers=self._json_headers,
            params=self._api_params(),
            json={"query": wiql},
        )
        resp.raise_for_status()
        work_items = resp.json().get("workItems", [])
        if not work_items:
            self._cache.set(key, [])
            return []

        # Fetch in batches of 200 (TFS API limit)
        all_details = []
        details_url = f"{self.org_url}/_apis/wit/workitems"
        for i in range(0, len(work_items), 200):
            batch = work_items[i:i + 200]
            ids = ",".join(str(wi["id"]) for wi in batch)
            details_resp = requests.get(
                details_url,
                headers=self._json_headers,
                params=self._api_params({"ids": ids, "$expand": "relations"}),
            )
            details_resp.raise_for_status()
            all_details.extend(details_resp.json().get("value", []))

        self._cache.set(key, all_details)
        return all_details
