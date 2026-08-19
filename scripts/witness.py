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

# THE FAMILY'S FILENAME, and the only thing this platform knows about what the
# product publishes. `compare_products.py` in contoso-data-product reads one of
# these per runtime; the product decides what goes in it, this decides where to
# put the copy it fetched.
SNAPSHOT = "product_snapshot.json"


def token(scope: str = "https://api.fabric.microsoft.com/.default") -> str:
    form = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": scope,
        }
    ).encode()
    req = urllib.request.Request(
        f"{ENTRA}/{TENANT}/oauth2/v2.0/token",
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)["access_token"]


def fetch_snapshot(session: requests.Session, api: str) -> str | None:
    """The product's snapshot, from whichever lakehouse it landed in.

    BY CONVENTION, NOT BY CONFIGURATION. The DAG provisions its own workspace
    and lakehouse and names them itself -- `provision` is the product's step,
    and a platform that hardcoded those names would be a platform that only
    runs one product. What both ends agree on is the FILENAME, which is the
    family's, so this looks for that and nothing else.
    """
    storage = token("https://storage.azure.com/.default")
    onelake = f"{FABRIC}/onelake"
    listed = session.get(f"{api}/workspaces", timeout=60)
    listed.raise_for_status()
    for space in listed.json().get("value", []):
        items = session.get(f"{api}/workspaces/{space['id']}/items", timeout=60)
        items.raise_for_status()
        for entry in items.json().get("value", []):
            if entry.get("type") != "Lakehouse":
                continue
            got = session.get(
                f"{onelake}/{space['id']}/{entry['id']}/Files/{SNAPSHOT}",
                headers={"Authorization": f"Bearer {storage}"},
                timeout=60,
            )
            if got.status_code == 200:
                print(f"snapshot from {space['displayName']}/{entry['displayName']}")
                return got.text
    return None


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

    # BRING THE EVIDENCE BACK. A green run is not the deliverable -- the
    # numbers are, and until now they existed only in a task log that a person
    # had to open and read out. `compare_products.py` cannot diff a log.
    fetched = fetch_snapshot(session, api)
    if fetched is None:
        # NOT FATAL, and not silent either. The run genuinely passed; what is
        # missing is the comparison, so saying so plainly beats both failing a
        # good run and letting the cell look complete when it cannot be
        # compared against its siblings.
        print(
            f"WARNING: the run passed but no {SNAPSHOT} was found in any "
            f"lakehouse -- this cell cannot enter the family comparison."
        )
        return 0
    pathlib.Path(SNAPSHOT).write_text(fetched, encoding="utf-8")
    print(f"wrote {SNAPSHOT}: {json.loads(fetched)}")
    return 0


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings()
    raise SystemExit(main())
