"""Non-destructive foundation reconciliation through the public Fabric API."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable, Protocol
from urllib.parse import urlparse
from uuid import UUID

from fabric_etl.contracts import ContractError, Environment, required_text


API = "https://api.fabric.microsoft.com/v1"


class DeploymentError(RuntimeError):
    """Deployment failed or its completion could not be established."""


@dataclass(frozen=True)
class Response:
    status: int
    body: object
    headers: dict[str, str]


class Transport(Protocol):
    def __call__(self, method: str, url: str, body: object | None, timeout: float) -> Response: ...


def api_url(url: str) -> str:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https" or parsed.netloc != "api.fabric.microsoft.com"
        or not parsed.path.startswith("/v1/") or parsed.fragment
    ):
        raise DeploymentError("Refusing to send credentials outside the Fabric API")
    return url


class FoundationDeployer:
    def __init__(
        self, transport: Transport, *, timeout_seconds: float = 600,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ContractError("Deployment timeout must be positive")
        self.transport = transport
        self.timeout_seconds = timeout_seconds
        self.clock = clock
        self.sleep = sleep
        self.deadline = 0.0

    def _remaining(self) -> float:
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise DeploymentError("Fabric deployment deadline exceeded; inspect remote state before retry")
        return remaining

    def _pause(self, response: Response) -> None:
        retry_after = response.headers.get("retry-after", "2")
        try:
            delay = max(1, int(retry_after))
        except ValueError as exc:
            raise DeploymentError("Invalid Fabric Retry-After header") from exc
        self.sleep(min(delay, self._remaining()))

    def _request(self, method: str, url: str, body: object | None = None) -> Response:
        api_url(url)
        while True:
            response = self.transport(method, url, body, min(60, self._remaining()))
            self._remaining()
            if method == "GET" and response.status in {429, 502, 503, 504}:
                self._pause(response)
                continue
            if response.status >= 400:
                code = "UnknownError"
                if isinstance(response.body, dict):
                    candidate = response.body.get("errorCode")
                    if isinstance(candidate, str):
                        code = candidate
                raise DeploymentError(f"Fabric {method} failed: HTTP {response.status}, {code}")
            return response

    def _get(self, url: str) -> dict:
        response = self._request("GET", url)
        if response.status != 200 or not isinstance(response.body, dict):
            raise DeploymentError("Unexpected Fabric GET result")
        return response.body

    def _list(self, workspace_id: str) -> list[dict]:
        url = f"{API}/workspaces/{workspace_id}/lakehouses"
        result: list[dict] = []
        seen: set[str] = set()
        while url:
            if url in seen:
                raise DeploymentError("Fabric pagination loop detected")
            seen.add(url)
            page = self._get(url)
            values = page.get("value")
            if not isinstance(values, list) or any(not isinstance(v, dict) for v in values):
                raise DeploymentError("Fabric lakehouse list has an invalid shape")
            result.extend(values)
            continuation = page.get("continuationUri")
            if continuation is None:
                break
            url = api_url(required_text(continuation, "continuationUri"))
        return result

    def _create(self, workspace_id: str, name: str) -> str:
        # Never retry POST: a lost response can still mean a committed create.
        response = self._request("POST", f"{API}/workspaces/{workspace_id}/lakehouses", {
            "displayName": name, "description": "Managed Fabric ETL template foundation",
            "creationPayload": {"enableSchemas": True},
        })
        if response.status == 201 and isinstance(response.body, dict):
            return str(UUID(required_text(response.body.get("id"), "created lakehouse ID")))
        if response.status != 202:
            raise DeploymentError("Unexpected lakehouse creation result; reconcile before retry")
        operation = required_text(response.headers.get("location"), "operation Location")
        api_url(operation)
        while True:
            self._pause(response)
            response = self._request("GET", operation)
            if response.status != 200 or not isinstance(response.body, dict):
                raise DeploymentError("Invalid Fabric operation response")
            status = response.body.get("status")
            if status == "Succeeded":
                result = self._get(operation.rstrip("/") + "/result")
                return str(UUID(required_text(result.get("id"), "created lakehouse ID")))
            if status in {"Failed", "Cancelled"}:
                raise DeploymentError(f"Lakehouse provisioning operation {status}")
            if status not in {"NotStarted", "Running"}:
                raise DeploymentError(f"Unknown Fabric operation status: {status!r}")

    def reconcile(self, environment: Environment) -> list[dict]:
        self.deadline = self.clock() + self.timeout_seconds
        existing = self._list(environment.workspace_id)
        outcomes: list[dict] = []
        for role, binding in environment.lakehouses.items():
            matches = [
                item for item in existing
                if isinstance(item.get("displayName"), str)
                and item["displayName"].casefold() == binding.name.casefold()
            ]
            if len(matches) > 1:
                raise DeploymentError(f"Ambiguous existing lakehouse: {binding.name}")
            if binding.item_id is not None:
                item_id = binding.item_id
                action = "verified"
                if matches and matches[0].get("id") != item_id:
                    raise DeploymentError(f"Pinned ID/name conflict for {role}")
            elif matches:
                item_id = str(UUID(required_text(matches[0].get("id"), "lakehouse ID")))
                action = "reused"
            else:
                item_id = self._create(environment.workspace_id, binding.name)
                action = "created"
            url = f"{API}/workspaces/{environment.workspace_id}/lakehouses/{item_id}"
            while True:
                item = self._get(url)
                if (
                    item.get("id") != item_id or item.get("displayName") != binding.name
                    or item.get("workspaceId") != environment.workspace_id
                    or item.get("type") != "Lakehouse"
                ):
                    raise DeploymentError(f"Remote lakehouse identity mismatch for {role}")
                properties = item.get("properties")
                if not isinstance(properties, dict) or not properties.get("defaultSchema"):
                    raise DeploymentError(f"Schema-enabled lakehouse required for {role}")
                endpoint = properties.get("sqlEndpointProperties")
                if not isinstance(endpoint, dict):
                    raise DeploymentError(f"SQL endpoint result missing for {role}")
                state = endpoint.get("provisioningStatus")
                if state == "Success":
                    endpoint_id = str(UUID(required_text(endpoint.get("id"), "SQL endpoint ID")))
                    break
                if state not in {"InProgress", "NotStarted"}:
                    raise DeploymentError(f"Unexpected SQL endpoint state for {role}: {state!r}")
                self.sleep(min(2, self._remaining()))
            outcomes.append({
                "role": role, "name": binding.name, "item_id": item_id,
                "sql_endpoint_id": endpoint_id, "action": action,
                "schema_enabled": True, "sql_endpoint_status": state,
            })
        return outcomes
