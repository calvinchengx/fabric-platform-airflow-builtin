#!/usr/bin/env python3
"""Drive a product's DAG through Fabric's built-in Airflow, end to end.

WHAT THIS PROVES that the emulator's own e2e does not: that a PLATFORM can do
it. The emulator's suite calls its own API from a script that lives beside it;
this runs the same four calls out of `platform/airflowjob.py`, from a separate
repository, against a pinned published image — which is the thing a person
cloning this repo actually needs to work.

    python scripts/witness.py <product-dir>

The product supplies `dags/`. This file knows nothing else about it.
"""

from __future__ import annotations

import json
import pathlib
import sys
import urllib.parse
import urllib.request

import requests

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "platform"))

import airflowjob  # noqa: E402

ENTRA = "http://localhost:18443"
FABRIC = "https://localhost:19443"
TENANT = "6f89cf12-978b-4d23-ac18-9ef0c127cf87"
# The emulator's seeded daemon app. In real Fabric these are the customer's
# service principal, which is why they are read from the environment first.
CLIENT_ID = "00d88624-f0d7-46f6-a641-6232c2608928"
CLIENT_SECRET = "daemon-app-secret"


def token() -> str:
    form = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://api.fabric.microsoft.com/.default",
        }
    ).encode()
    req = urllib.request.Request(
        f"{ENTRA}/{TENANT}/oauth2/v2.0/token",
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)["access_token"]


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit("usage: witness.py <product-dir>")
    product = pathlib.Path(sys.argv[1]).resolve()
    dags = product / "dags"
    if not dags.is_dir():
        sys.exit(f"{product} has no dags/ -- a product for this platform supplies one")

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token()}"
    # The emulator's certificate is self-signed; real Fabric is a public CA and
    # this line is the only difference, which is why it is here and not
    # threaded through the driver.
    session.verify = False
    api = f"{FABRIC}/v1"

    # IDEMPOTENT BY NAME, like the item. A second run should drive the
    # workspace the first one made, not fail on 409 -- which is exactly what
    # this did, because the item was made re-runnable and the workspace above
    # it was not.
    workspace = ""
    listed = session.get(f"{api}/workspaces", timeout=60)
    listed.raise_for_status()
    for existing in listed.json().get("value", []):
        if existing.get("displayName") == product.name:
            workspace = existing["id"]
            break
    if not workspace:
        created = session.post(
            f"{api}/workspaces", json={"displayName": product.name}, timeout=60
        )
        created.raise_for_status()
        workspace = created.json()["id"]

    item = airflowjob.create(session, api, workspace, f"{product.name}-orchestrator")
    published = airflowjob.publish_dags(session, api, workspace, item, dags)
    print(f"published {len(published)} file(s): {', '.join(published)}")

    dag_id = pathlib.Path(published[0]).stem
    print(f"running dag {dag_id!r} ...")
    state = airflowjob.run(session, api, workspace, item, dag_id, conf={"source": "witness"})
    print(f"PASS: Fabric's built-in Airflow ran {dag_id!r} -> {state.get('status')}")
    return 0


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings()
    raise SystemExit(main())
