"""Bounded item deployment and job feedback using supported Fabric APIs."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
from pathlib import Path
from uuid import UUID
from urllib.parse import urlparse

from fabric_etl.contracts import ContractError, fingerprint, load_json, required_text
from fabric_etl.deployment import API, DeploymentError, FoundationDeployer, Response, api_url


class JobFailure(DeploymentError):
    def __init__(self, job: dict) -> None:
        self.job = job
        super().__init__(f"Notebook job {job.get('status')}: {job.get('failureReason')}")


class ItemClient(FoundationDeployer):
    def start(self) -> None:
        self.deadline = self.clock() + self.timeout_seconds

    def operation(self, response: Response, *, require_result: bool = True) -> object:
        if response.status != 202:
            if response.status not in {200, 201}:
                raise DeploymentError(f"Unexpected operation response: {response.status}")
            return response.body
        operation_id = response.headers.get("x-ms-operation-id")
        if operation_id:
            location = f"{API}/operations/{UUID(operation_id)}"
        else:
            location = api_url(required_text(response.headers.get("location"), "operation Location"))
        while True:
            self._pause(response)
            response = self._request("GET", location)
            if response.status != 200 or not isinstance(response.body, dict):
                raise DeploymentError("Invalid long-running operation response")
            state = response.body.get("status")
            if state == "Succeeded":
                return self._get(location.rstrip("/") + "/result") if require_result else response.body
            if state in {"Failed", "Cancelled"}:
                raise DeploymentError(f"Fabric operation {state}: {response.body.get('error')}")
            if state not in {"Running", "NotStarted"}:
                raise DeploymentError(f"Unknown operation state: {state}")

    def list_items(self, workspace_id: str) -> list[dict]:
        url = f"{API}/workspaces/{UUID(workspace_id)}/items"
        result = []
        seen = set()
        while url:
            if url in seen:
                raise DeploymentError("Item pagination loop")
            seen.add(url)
            page = self._get(url)
            rows = page.get("value")
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise DeploymentError("Invalid item list")
            result.extend(rows)
            continuation = page.get("continuationUri")
            url = api_url(required_text(continuation, "continuationUri")) if continuation else ""
        return result

    def assert_exclusive_setup(self, workspace_id: str) -> None:
        url = f"{API}/workspaces/{UUID(workspace_id)}/spark/livySessions"
        seen = set()
        while url:
            if url in seen:
                raise DeploymentError("Spark session pagination loop")
            seen.add(url)
            page = self._get(url)
            values = page.get("value")
            if not isinstance(values, list):
                raise DeploymentError("Cannot establish exclusive setup: invalid Spark session list")
            for session in values:
                if session.get("state") not in {"Succeeded", "Cancelled", "Failed", "Completed"}:
                    raise DeploymentError("Exclusive setup blocked by active or uncertain Spark session")
            continuation = page.get("continuationUri")
            url = api_url(continuation) if continuation else ""

    def definition(self, workspace_id: str, item_id: str, item_type: str) -> dict:
        plural = {"Environment": "environments", "Notebook": "notebooks"}[item_type]
        response = self._request(
            "POST", f"{API}/workspaces/{workspace_id}/{plural}/{item_id}/getDefinition"
            + ("?format=ipynb" if item_type == "Notebook" else ""),
        )
        result = self.operation(response)
        if not isinstance(result, dict) or not isinstance(result.get("definition"), dict):
            raise DeploymentError("Missing exported item definition")
        return result["definition"]

    def verify_published_library(
        self, workspace_id: str, environment_id: str, library_name: str, expected_sha256: str,
    ) -> None:
        item = self._get(f"{API}/workspaces/{workspace_id}/environments/{environment_id}")
        if (
            item.get("id") != environment_id
            or item.get("workspaceId") != workspace_id
            or item.get("type") != "Environment"
        ):
            raise DeploymentError("Pinned phase-five Environment identity differs")
        state = item.get("properties", {}).get("publishDetails", {}).get("state")
        if state != "Success":
            raise DeploymentError(f"Phase-five Environment is not published: {state!r}")
        parts = decode_parts(self.definition(workspace_id, environment_id, "Environment"))
        path = f"Libraries/CustomLibraries/{library_name}"
        payload = parts.get(path)
        if payload is None or hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise DeploymentError("Published phase-five library is missing or its checksum differs")

    def publish_environment(self, workspace_id: str, item_id: str) -> dict:
        url = f"{API}/workspaces/{workspace_id}/environments/{item_id}"
        item = self._get(url)
        state = item.get("properties", {}).get("publishDetails", {}).get("state")
        if state not in {"Running", "Waiting"}:
            response = self._request("POST", url + "/staging/publish?beta=false")
            if response.status == 202:
                self.operation(response, require_result=False)
            elif response.status != 200:
                raise DeploymentError("Unexpected publish response")
        while True:
            item = self._get(url)
            details = item.get("properties", {}).get("publishDetails", {})
            state = details.get("state")
            if state == "Success":
                return details
            if state not in {"Running", "Waiting"}:
                raise DeploymentError(f"Environment publish did not succeed: {details}")
            self.sleep(min(15, self._remaining()))

    def submit_notebook(
        self, workspace_id: str, item_id: str, parameters: dict, *, environment_id: str,
        lakehouse_id: str,
    ) -> str:
        response = self._request(
            "POST", f"{API}/workspaces/{workspace_id}/notebooks/{item_id}/jobs/execute/instances?beta=false",
            {
                "parameters": [{"name": name, "value": value, "type": "Text"}
                               for name, value in parameters.items()],
                "executionData": {
                    "compute": "Spark", "computeConfiguration": {
                        "attachedEnvironment": {
                            "referenceType": "ById", "itemId": str(UUID(environment_id)),
                            "workspaceId": workspace_id,
                        },
                        "defaultLakehouse": {
                            "referenceType": "ById", "itemId": str(UUID(lakehouse_id)),
                            "workspaceId": workspace_id,
                        },
                        "highConcurrencyModeOptions": {"enabled": False},
                    },
                },
            },
        )
        if response.status != 202:
            raise DeploymentError("Notebook submission not accepted")
        location = urlparse(required_text(response.headers.get("location"), "job Location"))
        instance_id = str(UUID(location.path.rstrip("/").split("/")[-1]))
        url = f"{API}/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{instance_id}"
        return url

    def wait_job(self, url: str) -> dict:
        while True:
            job = self._get(api_url(url))
            state = job.get("status")
            if state == "Completed":
                match = re.fullmatch(
                    r"/v1/workspaces/([0-9a-f-]+)/items/([0-9a-f-]+)/jobs/instances/([0-9a-f-]+)",
                    urlparse(url).path,
                )
                if match is None:
                    raise DeploymentError("Invalid persisted notebook job URL")
                workspace, item, instance = (str(UUID(value)) for value in match.groups())
                detailed = self._get(
                    f"{API}/workspaces/{workspace}/notebooks/{item}/jobs/execute/instances/{instance}?beta=true",
                )
                properties = detailed.get("properties")
                exit_value = detailed.get("exitValue")
                if not isinstance(exit_value, str) and isinstance(properties, dict):
                    exit_value = properties.get("exitValue")
                if not isinstance(exit_value, str) or not exit_value:
                    raise DeploymentError("Completed notebook has no explicit exit value; runtime proof missing")
                result = json.loads(exit_value)
                if not isinstance(result, dict) or "status" not in result or "scope" not in result:
                    raise DeploymentError("Notebook exit contract invalid")
                return {**job, "exit_result": result}
            if state in {"Failed", "Cancelled", "Deduped"}:
                raise JobFailure(job)
            if state not in {"NotStarted", "InProgress"}:
                raise DeploymentError(f"Unknown notebook job state: {state}")
            self.sleep(min(15, self._remaining()))


def decode_parts(definition: dict) -> dict[str, bytes]:
    parts = definition.get("parts")
    if not isinstance(parts, list) or not parts:
        raise ContractError("Item definition has no parts")
    result = {}
    for part in parts:
        if not isinstance(part, dict) or part.get("payloadType") != "InlineBase64":
            raise ContractError("Invalid definition part")
        path = required_text(part.get("path"), "definition part path")
        if path in result:
            raise ContractError("Duplicate exported part")
        result[path] = base64.b64decode(required_text(part.get("payload"), "payload"), validate=True)
    return result


def normalized_settings(data: bytes) -> str:
    text = data.decode("utf-8").replace("\r", "")
    lines = []
    for line in text.splitlines():
        if line.strip() == "instance_pool_id: null":
            continue
        if line.startswith("runtime_version:"):
            line = "runtime_version: " + line.split(":", 1)[1].strip().strip("'\"")
        if line.strip():
            lines.append(line.rstrip())
    return "\n".join(lines)


def notebook_definition(source: str) -> dict:
    sections = source.split("# METADATA ********************")
    metadata_lines = [
        line.removeprefix("# META ") for line in sections[1].splitlines()
        if line.startswith("# META ")
    ]
    metadata = json.loads("\n".join(metadata_lines))
    chunks = source.split("# CELL ********************")
    # Parameter cells have a distinct Git-source marker.
    first = source.split("# PARAMETERS CELL ********************")
    if len(first) != 2 or len(chunks) != 2:
        raise ContractError("Thin notebook must contain one parameter and one execution cell")
    parameters = first[1].split("# METADATA ********************", 1)[0].strip() + "\n"
    execution = chunks[1].split("# METADATA ********************", 1)[0].strip() + "\n"
    notebook = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
            "language_info": {"name": "python"},
            "dependencies": metadata["dependencies"],
        },
        "cells": [
            {"cell_type": "code", "source": code.splitlines(keepends=True),
             "metadata": {"tags": ["parameters"]} if index == 0 else {},
             "execution_count": None, "outputs": []}
            for index, code in enumerate((parameters, execution))
        ],
    }
    return {"format": "ipynb", "parts": [{
        "path": "artifact.content.ipynb", "payloadType": "InlineBase64",
        "payload": base64.b64encode(json.dumps(notebook).encode()).decode(),
    }]}


def verify_notebook(actual: bytes, expected: bytes) -> None:
    remote, local = json.loads(actual), json.loads(expected)
    remote_cells, local_cells = remote.get("cells"), local["cells"]
    if not isinstance(remote_cells, list) or len(remote_cells) != len(local_cells):
        raise DeploymentError("Notebook export cell inventory differs")
    for observed, requested in zip(remote_cells, local_cells):
        observed_source = observed.get("source", [])
        if isinstance(observed_source, list):
            observed_source = "".join(observed_source)
        if observed_source.replace("\r\n", "\n").strip() != "".join(requested["source"]).replace("\r\n", "\n").strip():
            raise DeploymentError("Notebook exported executable source differs")
        if observed.get("metadata", {}).get("tags", []) != requested["metadata"].get("tags", []):
            raise DeploymentError("Notebook parameter tags differ")
    dependencies = remote.get("metadata", {}).get("dependencies", {})
    for key, value in local["metadata"]["dependencies"].items():
        if dependencies.get(key) != value:
            raise DeploymentError("Notebook dependency binding differs")


def deploy_items(client: ItemClient, plan: dict, journal: Path) -> dict:
    expected = fingerprint({k: v for k, v in plan.items() if k != "plan_id"})
    if plan.get("plan_id") != expected or plan.get("status") != "PLANNED_NOT_DEPLOYED":
        raise ContractError("Invalid local plan fingerprint/status")
    workspace = str(UUID(plan["workspace_id"]))
    client.start()
    previous = load_json(journal) if journal.exists() else {
        "workspace_id": workspace, "items": {},
    }
    if previous.get("workspace_id") != workspace:
        raise ContractError("Deployment journal workspace mismatch")
    records = previous["items"]
    existing = client.list_items(workspace)
    for template in plan["items"]:
        item = copy.deepcopy(template)
        key, kind = item["id"], item["type"]
        parts = decode_parts(item["definition"])
        if kind == "Notebook":
            binding = records.get("etl_environment") or plan.get("environment_binding")
            if not binding:
                raise ContractError("Notebook Environment prerequisite missing")
            if binding.get("workspace_id", workspace) != workspace:
                raise ContractError("Notebook Environment belongs to a different workspace")
            source = parts["notebook-content.py"].decode("utf-8")
            if source.count("${etl_environment_item_id}") != 1:
                raise ContractError("Notebook Environment placeholder missing or ambiguous")
            item["definition"] = notebook_definition(
                source.replace("${etl_environment_item_id}", binding["item_id"]),
            )
        matches = [entry for entry in existing
                   if entry.get("displayName", "").casefold() == item["displayName"].casefold()]
        if len(matches) > 1 or (matches and matches[0].get("type") != kind):
            raise DeploymentError("Ambiguous item name or wrong existing type")
        pinned = records.get(key)
        if pinned and (not matches or matches[0]["id"] != pinned["item_id"]):
            raise DeploymentError("Pinned item binding changed")
        plural = {"Environment": "environments", "Notebook": "notebooks"}[kind]
        digest = fingerprint(item["definition"])
        if not matches:
            created = client.operation(client._request(
                "POST", f"{API}/workspaces/{workspace}/{plural}",
                {"displayName": item["displayName"], "definition": item["definition"],
                 "description": "Managed Fabric ETL phase-four item"},
            ))
            if not isinstance(created, dict):
                raise DeploymentError("Create did not return item identity")
            item_id = str(UUID(required_text(created.get("id"), "item ID")))
            action = "created"
        else:
            item_id = str(UUID(matches[0]["id"]))
            if not pinned:
                raise DeploymentError("Existing item is not owned by this deployment journal")
            action = "verified"
            if pinned["definition_fingerprint"] != digest:
                client.operation(client._request(
                    "POST", f"{API}/workspaces/{workspace}/{plural}/{item_id}/updateDefinition",
                    {"definition": item["definition"]},
                ), require_result=False)
                action = "updated"
        records[key] = {
            **(pinned or {}),
            "item_id": item_id, "type": kind, "display_name": item["displayName"],
            "definition_fingerprint": digest, "action": action, "verified": False,
        }
        journal.parent.mkdir(parents=True, exist_ok=True)
        # Persist the identity before publishing or verifying; do not recreate on recovery.
        journal.write_bytes(json.dumps(previous, indent=2).encode())
        if kind == "Environment" and (action != "verified" or not pinned or not pinned.get("verified")):
            records[key]["publication_requested"] = True
            journal.write_bytes(json.dumps(previous, indent=2).encode())
            records[key]["publish"] = client.publish_environment(workspace, item_id)
        exported = decode_parts(client.definition(workspace, item_id, kind))
        requested = decode_parts(item["definition"])
        for path, payload in requested.items():
            export_path = "notebook-content.ipynb" if path == "artifact.content.ipynb" else path
            actual = exported.get(export_path)
            if actual is None:
                raise DeploymentError(f"Export missing managed part: {path}")
            if path.endswith(".whl") and hashlib.sha256(actual).digest() != hashlib.sha256(payload).digest():
                raise DeploymentError("Published library checksum differs")
            if path == "Setting/Sparkcompute.yml" and normalized_settings(actual) != normalized_settings(payload):
                raise DeploymentError("Exported Spark settings differ from declared configuration")
            if path.endswith(".py") and actual.replace(b"\r\n", b"\n").strip() != payload.replace(b"\r\n", b"\n").strip():
                raise DeploymentError("Notebook export differs from requested source")
            if path.endswith(".ipynb"):
                verify_notebook(actual, payload)
        records[key]["verified"] = True
        records[key]["exported_part_sha256"] = {
            path: hashlib.sha256(data).hexdigest() for path, data in exported.items()
        }
        previous["plan_id"] = plan["plan_id"]
        journal.write_bytes(json.dumps(previous, indent=2).encode())
    return previous
