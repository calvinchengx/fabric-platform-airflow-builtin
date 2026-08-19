"""Drive an `ApacheAirflowJob` item: publish DAGs, run one, wait for it.

THIS IS THE WHOLE PLATFORM'S REASON TO EXIST. Fabric's built-in orchestrator is
an item like any other -- you create it, you PUT files into it, and you start a
job on it -- and the DAG that runs is the PRODUCT'S, uploaded from the
product's own `dags/` directory. The platform never reads what the DAG does.

WHY THIS IS NOT THE SAME AS THE AIRFLOW 3 PLATFORM. There, the platform runs
Airflow itself and mounts the product as a DAG bundle; the scheduler is the
platform's. Here the scheduler belongs to FABRIC: the item is the unit of
deployment, files are pushed through the control plane, and a run is a job
instance with a status you poll. Nothing about that is emulator-specific --
against real Fabric the same four calls do the same four things, which is the
property this platform exists to demonstrate.

THE FILES API IS `?beta=true` AT THE TIME OF WRITING. That is Fabric's own
preview marker, not the emulator's invention, and it is spelled here once so a
GA transition is a one-line change rather than a search.
"""

from __future__ import annotations

import pathlib
import time

import requests

# Fabric's own preview marker on the ApacheAirflowJob files API.
FILES_QUERY = "?beta=true"

# A job instance ends in one of these. Anything else means keep waiting --
# and an UNKNOWN state is NOT treated as terminal, because a status this
# platform does not recognise is a reason to fail loudly rather than to guess
# which way it went.
TERMINAL = {"Completed", "Failed", "Cancelled", "Deduped"}


class JobFailed(RuntimeError):
    """A run reached a terminal state that was not Completed."""


def create(session: requests.Session, api: str, workspace: str, name: str) -> str:
    """Create the ApacheAirflowJob item and return its id.

    Idempotent by NAME rather than by catching a conflict: a second `make
    verify` should drive the item it made last time, not accumulate one per
    run. Fabric lists items per workspace, so the lookup is the same call a
    person would make.
    """
    listed = session.get(f"{api}/workspaces/{workspace}/items", timeout=60)
    listed.raise_for_status()
    for item in listed.json().get("value", []):
        if item.get("displayName") == name and item.get("type") == "ApacheAirflowJob":
            return item["id"]
    created = session.post(
        f"{api}/workspaces/{workspace}/items",
        json={"displayName": name, "type": "ApacheAirflowJob"},
        timeout=60,
    )
    created.raise_for_status()
    return created.json()["id"]


def publish_dags(
    session: requests.Session, api: str, workspace: str, item: str, dags: pathlib.Path,
) -> list[str]:
    """PUT every .py under `dags/` into the item, and verify what landed.

    READ BACK AFTER WRITING, and compare sizes. A silently empty upload is the
    failure this guards: the item would exist, the job would start, and Airflow
    would report "DAG not found" -- three steps from the upload that did
    nothing. The emulator syncs these files into the scheduler's DAG folder, so
    what is listed here is what Airflow will parse.
    """
    files = f"{api}/workspaces/{workspace}/apacheAirflowJobs/{item}/files"
    sent: dict[str, int] = {}
    for path in sorted(dags.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(dags.parent).as_posix()
        body = path.read_bytes()
        put = session.put(
            f"{files}/{rel}{FILES_QUERY}",
            data=body,
            headers={"Content-Type": "application/octet-stream"},
            timeout=300,
        )
        put.raise_for_status()
        sent[rel] = len(body)

    if not sent:
        raise RuntimeError(
            f"no DAGs under {dags} -- the product supplied nothing to run, and "
            f"a job started against an empty item fails as 'DAG not found', "
            f"which names neither this directory nor the product"
        )

    listed = session.get(f"{files}{FILES_QUERY}", timeout=60)
    listed.raise_for_status()
    landed = {f["filePath"]: f["sizeInBytes"] for f in listed.json().get("value", [])}
    missing = {k: v for k, v in sent.items() if landed.get(k) != v}
    if missing:
        raise RuntimeError(
            f"the item did not store what was sent: {missing} against {landed}"
        )

    # NO WAIT HERE, DELIBERATELY, and it is not an omission.
    #
    # This slept 45 seconds before returning, because a changed DAG published
    # and triggered at once came back as the PREVIOUS version. The sleep was on
    # the wrong side of the problem: `PUT .../files` only stores bytes on the
    # item, and the write into the scheduler's DAG folder happens in the
    # emulator's Run handler immediately before it triggers -- so this slept
    # before the files existed on disk at all.
    #
    # The number kept moving because nobody had identified what it was racing.
    # 15s let four stale runs through and 45s appeared to fix it; both sit
    # either side of Airflow's `min_serialized_dag_update_interval` (30s), which
    # lets a parse read a file and skip rewriting the serialised DAG that task
    # instances actually come from.
    #
    # Closed in fabric-emulator v0.31.0, which reads the task set before syncing
    # and waits for it to change. A sleep reappearing here would be a
    # workaround re-derived for a defect that is fixed -- see the pin test.
    return sorted(sent)


def run(
    session: requests.Session,
    api: str,
    workspace: str,
    item: str,
    dag_id: str,
    conf: dict | None = None,
    timeout: float = 1800.0,
    poll: float = 5.0,
) -> dict:
    """Start a Run job on the item and poll until it is terminal.

    `executionData.dagId` is how Fabric names WHICH DAG in the item to run --
    an item can hold several, so the job is not "run the item", it is "run this
    graph". `conf` rides along as Airflow's own dag_run conf.
    """
    started = session.post(
        f"{api}/workspaces/{workspace}/items/{item}/jobs/instances?jobType=Run",
        json={"executionData": {"dagId": dag_id, **({"conf": conf} if conf else {})}},
        timeout=60,
    )
    if started.status_code not in (200, 202):
        raise JobFailed(f"could not start {dag_id}: {started.status_code} {started.text[:300]}")

    # Fabric returns the instance in Location; the emulator answers with the
    # body as well. Prefer the header, because that is the documented contract.
    location = started.headers.get("Location")
    instance = location.rsplit("/", 1)[-1] if location else (started.json() or {}).get("id")
    if not instance:
        raise JobFailed(f"no job instance id in {started.headers} / {started.text[:200]}")

    deadline = time.monotonic() + timeout
    url = f"{api}/workspaces/{workspace}/items/{item}/jobs/instances/{instance}"
    while True:
        got = session.get(url, timeout=60)
        got.raise_for_status()
        state = got.json()
        status = state.get("status")
        if status in TERMINAL:
            if status != "Completed":
                raise JobFailed(f"{dag_id} finished {status}: {state}")
            return state
        if time.monotonic() > deadline:
            raise JobFailed(
                f"{dag_id} was still {status!r} after {timeout:.0f}s. The DAG may be "
                f"waiting on a task that cannot run -- check the Airflow UI the "
                f"platform prints, rather than raising this timeout."
            )
        time.sleep(poll)
