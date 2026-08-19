"""Repo-boundary tests. No Docker, no emulator, no credentials."""

from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
COMPOSE = (ROOT / "compose" / "docker-compose.yml").read_text(encoding="utf-8")


def pins() -> dict[str, str]:
    out = {}
    for line in (ROOT / "versions.env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def test_pins_are_immutable():
    for key, value in pins().items():
        assert value.lower() not in {"latest", "stable", "main", "edge"}, f"{key}={value}"


def test_compose_reads_every_pin():
    for key in pins():
        assert "${" + key in COMPOSE, f"{key} is pinned but nothing reads it"


def test_the_sidecar_matches_what_fabric_runs():
    """Airflow 2.10.5 / Python 3.12 and CeleryExecutor, because Fabric says so.

    Microsoft supports exactly one Airflow version for Apache Airflow jobs and
    lists `AIRFLOW__CORE__EXECUTOR` among the settings a user CANNOT override;
    its value there is CeleryExecutor. So neither is this platform's choice,
    and a drift is a fidelity bug rather than a preference.

    SequentialExecutor is called out by name because it is the one that fails
    quietly: it runs a parallel DAG one task at a time, so the run still passes
    while demonstrating behaviour no Fabric user can have.
    """
    assert pins()["AIRFLOW_VERSION"] == "2.10.5-python3.12"
    assert "AIRFLOW__CORE__EXECUTOR: CeleryExecutor" in COMPOSE
    assert "SequentialExecutor" not in COMPOSE
    assert "AIRFLOW__CELERY__BROKER_URL" in COMPOSE
    # A parallel executor on SQLite fails with `database is locked`.
    body = "\n".join(
        ln for ln in COMPOSE.splitlines() if not ln.lstrip().startswith("#")
    )
    assert "sqlite" not in body.lower()


def test_this_platform_runs_no_airflow_of_its_own_scheduling():
    """The scheduler is FABRIC'S, reached through ApacheAirflowJob items.

    The distinction from `fabric-platform-airflow3` is the whole point of this
    cell existing: there the platform owns the scheduler and mounts the product
    as a DAG bundle; here the item is the unit of deployment and a run is a job
    instance on Fabric's control plane. A DAG bundle configured here would mean
    this platform had quietly become the other one.
    """
    assert "AIRFLOW__CORE__DAGS_FOLDER" not in COMPOSE
    assert "dag_bundle" not in COMPOSE.lower() and "DAG_BUNDLE" not in COMPOSE
    assert "FABRIC_AIRFLOW_URL" in COMPOSE, (
        "without this the emulator's Airflow routes answer AirflowNotConfigured"
    )


def test_the_platform_carries_no_product():
    """No product name here. The product is named by PRODUCT= on the command line.

    SCOPED TO THE CODE THIS RULE IS ABOUT -- `platform/` and `scripts/` -- and
    not to the whole tree. The first version scanned everything and failed on
    ITSELF: a test that forbids a string has to contain that string, so it
    reported its own assertion as the violation. Excluding the test file by
    name would have been a workaround; the honest fix is that the rule was
    always about the platform's own code, and tests are not that.
    """
    offenders = []
    scanned = list((ROOT / "platform").rglob("*.py")) + list((ROOT / "scripts").rglob("*.py"))
    assert scanned, "nothing was scanned -- this test would pass on an empty repo"
    for path in scanned:
        if ".venv" in path.parts or "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        body = "\n".join(
            ln for ln in text.splitlines() if not ln.lstrip().startswith("#")
        )
        body = re.sub(r'"""(?:.|\n)*?"""', "", body)
        if "contoso" in body.lower():
            offenders.append(str(path.relative_to(ROOT)))
    assert offenders == [], f"a product name leaked into the platform: {offenders}"


def test_the_driver_refuses_an_empty_upload():
    """A job started against an item with no DAGs fails as 'DAG not found'.

    That message names neither the directory that was empty nor the product
    that failed to supply one, which is three steps from the cause. The driver
    refuses first.
    """
    src = (ROOT / "platform" / "airflowjob.py").read_text(encoding="utf-8")
    assert "no DAGs under" in src
    assert "the item did not store what was sent" in src, (
        "publish must read back what it wrote -- a silently empty upload is the "
        "failure this platform is most likely to hit"
    )


def test_an_unknown_job_status_is_not_treated_as_finished():
    """Only known terminal states end the poll.

    Treating an unrecognised status as terminal would report a run as finished
    on a status this platform has never seen -- the shape of guess that turns
    an emulator gap into a false green.
    """
    src = (ROOT / "platform" / "airflowjob.py").read_text(encoding="utf-8")
    assert "TERMINAL = {" in src
    for state in ("Completed", "Failed", "Cancelled"):
        assert f'"{state}"' in src


def test_publishing_waits_for_the_scheduler_to_reread():
    """A changed DAG published and triggered at once races re-serialisation.

    The run is created from whatever structure is currently serialised, so a
    newly added task comes back `removed` and its downstream fails -- a run
    that looks like a DAG bug and is not one. It happened twice before this
    wait existed.

    Asserted structurally because the failure only appears when a DAG CHANGES,
    which no unit test can stage and a steady-state integration run never hits.
    """
    src = (ROOT / "platform" / "airflowjob.py").read_text(encoding="utf-8")
    assert "reparse" in src, "publish_dags must give the scheduler time to re-read"
    assert "time.sleep(reparse)" in src
