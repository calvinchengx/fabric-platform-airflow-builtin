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
    # PYTHON_VERSION is read by the vendor generator rather than by compose:
    # it pins the interpreter this platform runs a vendor's own seed script
    # with, and that service is generated, not declared here. Still a pin this
    # platform owns -- just one whose consumer is a script.
    generated = {"PYTHON_VERSION"}
    sources = (ROOT / "scripts" / "sources.py").read_text(encoding="utf-8")
    for key in pins():
        if key in generated:
            assert key in sources, f"{key} is pinned but nothing reads it"
            continue
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


def test_publishing_does_not_sleep_before_running():
    """The race this used to guard is not a race a consumer can close.

    This platform slept 45 seconds between publishing a product's DAGs and
    starting a Run, because a changed DAG triggered immediately came back as
    the PREVIOUS version -- a run whose task instances belong to replaced code,
    which reads as a DAG bug and is not one.

    THE SLEEP WAS ON THE WRONG SIDE OF THE PROBLEM and could never have worked.
    `PUT .../files` only stores bytes on the item; the write into the
    scheduler's DAG folder happens in the emulator's Run handler, immediately
    before it triggers. Sleeping here sleeps before the files exist on disk at
    all. The race is entirely inside the emulator, between its own sync and its
    own trigger, and no amount of patience out here closes it.

    It also explains why the number kept moving. 15 seconds let four stale runs
    through and 45 appeared to fix it; both were guesses either side of
    Airflow's `min_serialized_dag_update_interval` (30s), which nobody had
    identified. A parse can read a file and skip rewriting the serialised DAG,
    and task instances come from that serialisation.

    Closed in fabric-emulator v0.31.0, which reads the DAG's task set before
    syncing and waits for it to change -- so this platform waits for nothing,
    and a sleep reappearing here would be someone re-deriving a workaround for
    a defect that is fixed.
    """
    import ast

    src = (ROOT / "platform" / "airflowjob.py").read_text(encoding="utf-8")
    # SCOPED TO publish_dags. `run` sleeps too, between polls of a job it is
    # waiting on, and that is a different thing entirely -- asserting against
    # the whole module would forbid polling as well as guessing.
    tree = ast.parse(src)
    publish = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "publish_dags"
    )
    sleeps = [
        node for node in ast.walk(publish)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "sleep"
    ]
    assert sleeps == [], (
        "the publish-to-run race belongs to the emulator and is fixed there; "
        "a sleep here buys nothing and hides that"
    )
    assert "reparse" not in ast.dump(publish)


def test_the_emulator_is_pinned_past_the_publish_race():
    """A platform that does not wait requires an emulator that does.

    Dropping the sleep is only safe against an emulator carrying the fix. Below
    v0.31.0 the trigger fires against whatever is serialised at that instant,
    and this platform would publish a DAG and run its previous version --
    silently, and green.
    """
    pins = (ROOT / "versions.env").read_text(encoding="utf-8")
    line = next(
        entry for entry in pins.splitlines()
        if entry.startswith("FABRIC_EMULATOR_VERSION=")
    )
    version = line.split("=", 1)[1].strip()
    major, minor, patch = (int(part) for part in version.split("."))
    assert (major, minor, patch) >= (0, 31, 0), (
        f"FABRIC_EMULATOR_VERSION={version} predates the publish-to-run fix; "
        f"without the sleep this platform runs stale DAGs"
    )


def test_the_platform_declares_no_vendor_of_its_own():
    """A hand-written vendor block drifts, and this one had.

    Both this platform and its siblings consume `contoso-sources`, so all of
    them pull the same vendor bytes from the same pinned simulator -- and gold
    agreeing across four runtimes means something only if the inputs were
    identical. A vendor declared here makes this platform's data ITS OWN and
    the family comparison starts measuring fixtures instead of runtimes.

    Three facts were wrong in the copy while the declaration was right: every
    vendor carried the POS system's 2GiB/4g budget, so a vendor serving four
    kilobytes held a 4 GB ceiling; a restart policy existed only here and was
    invisible to the three siblings running that same vendor; and the ERP's
    credentials were restated in three places.
    """
    compose = (ROOT / "compose" / "docker-compose.yml").read_text(encoding="utf-8")
    services = re.findall(r"^  ([a-z][a-z0-9-]*):$", compose, re.M)
    for vendor in ("contoso-pos", "contoso-web", "contoso-reference",
                   "contoso-erp-db", "contoso-erp-broker", "contoso-erp-connect",
                   "contoso-erp-seed"):
        assert vendor not in services, (
            f"{vendor} is declared in this platform's compose; it belongs to "
            f"contoso-sources/sources.yaml and is generated by `make sources`")
    # mokapi is what an OpenAPI vendor IS. Naming the image here would be this
    # platform deciding what the vendor is, one indirection from declaring it.
    assert "mokapi" not in compose
    assert "debezium" not in compose


def test_the_vendor_images_are_pinned_by_the_sources_repo_not_here():
    """A platform pinning a vendor's simulator is a platform deciding what the
    vendor is -- and two pins for one fact drift the moment either moves.

    POSTGRES_VERSION is exempt and stays: this platform runs its OWN postgres
    for Airflow's metadata database, which is a different decision from the ERP
    vendor's engine that happens to agree today.
    """
    pins = (ROOT / "versions.env").read_text(encoding="utf-8")
    for key in ("MOKAPI_VERSION", "REDPANDA_VERSION", "DEBEZIUM_VERSION"):
        assert f"\n{key}=" not in pins, (
            f"{key} is the vendor's, and contoso-sources/versions.env pins it")


def test_the_stack_is_generated_before_it_is_started():
    """`make up` without `make sources` starts an incomplete stack.

    Compose would not fail -- the fragment is simply absent, so the vendors
    never exist and the failure surfaces much later as an empty landing
    directory, which reads like a product bug.
    """
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "up: sources" in makefile, "generating the vendors must be a prerequisite of starting"
    assert "-f $(FRAGMENT)" in makefile


def test_up_refuses_a_product_it_cannot_build():
    """The default PRODUCT is a DAG-only fixture with no pyproject.toml.

    It exists for these tests, which never start Docker. A bare `make up`
    therefore fails inside buildkit on a missing file, naming neither the
    variable that was unset nor the fact that a default was used -- which cost
    a full teardown-and-rebuild cycle to diagnose.
    """
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert 'test -f "$(PRODUCT_ABS)/pyproject.toml"' in makefile
    assert not (ROOT / "tests" / "fixture-product" / "pyproject.toml").exists(), (
        "if the fixture gains a pyproject.toml this guard is no longer the reason "
        "a bare `make up` fails, and the message would mislead")


def test_a_vendor_is_healthy_only_when_it_enforces_its_credential():
    """Mokapi without its fixture does not fail -- it INVENTS.

    It generates response bodies from the OpenAPI schema and answers every
    request 200, wrong key included. A probe against `/` cannot tell that apart
    from a real vendor, so a healthcheck built that way reports green for a
    vendor serving fabricated data -- which would then flow through bronze,
    silver and gold and be compared against three other runtimes as if it were
    the product's numbers.

    The probe carries a key that is deliberately wrong, so it needs no secret
    and still learns the vendor is enforcing. The route is the declaration's,
    because which path enforces a credential is a fact about the vendor's API.
    """
    generator = (ROOT / "scripts" / "sources.py").read_text(encoding="utf-8")
    assert "definitely-not-the-key" in generator
    assert "v['health']" in generator, "the probe route must come from the declaration"
    # The inversion is the whole check: a 200 here means the vendor let a bad
    # key through, so wget SUCCEEDING is the failure.
    assert "&& exit 1 || exit 0" in generator


def test_the_sidecar_waits_for_the_emulators_certificate():
    """A cold start has no certificate yet, and a silent skip hides that.

    This platform built its trust bundle under a bare `if [ -f ... ]`, which on
    a stack started from EMPTY volumes is false: the bundle was never written,
    and the first symptom was `provision` failing with "Could not find a
    suitable TLS CA certificate bundle" -- four steps from the cause and naming
    neither the emulator nor the ordering.

    It survived every run until the first `make down -v`, because each start
    found a certificate left by an earlier one. That is the class of defect a
    stack which is never torn down cannot see, and the reason the fix is an
    explicit dependency rather than a longer sleep.
    """
    assert "fabric-emulator:\n        condition: service_healthy" in COMPOSE, (
        "airflow reads the emulator's certificate at startup, so it must wait for it")
    # And if the wait ever expires, it must fail loudly rather than start a
    # sidecar whose every Fabric call would fail obscurely.
    assert "never appeared" in COMPOSE
    assert "exit 1" in COMPOSE


def test_the_acceptance_run_asserts_the_numbers_and_not_only_the_run():
    """A nightly that proves the DAG RAN proves nothing about the answer.

    G50: across all seven platforms with an acceptance workflow, none compared a
    snapshot against an expected value. This cell has fetched
    product_snapshot.json out of OneLake since DoD 4 and nothing read it back,
    so gold could have returned different money indefinitely behind a green tick.

    THE FILENAME IS THE CONTRACT between witness.py and this step, and it is the
    same one the DAG writes. Asserting them together means a rename in the
    script cannot leave the workflow reading a file nobody produces.

    The core checkout must be PINNED. Left tracking main, this cell's
    expectations could move without a reviewed commit here -- a nightly that
    another repository can turn green.
    """
    raw = (ROOT / ".github" / "workflows" / "acceptance.yml").read_text(encoding="utf-8")
    wf = "\n".join(ln for ln in raw.splitlines() if not ln.lstrip().startswith("#"))
    assert "scripts/assert_snapshot.py" in wf, (
        "the acceptance run never asserts the figures core publishes"
    )
    core = wf[wf.index("repository: calvinchengx/contoso-data-product\n") :]
    assert re.search(r"ref: [0-9a-f]{40}", core[: core.index("path:")]), (
        "the contoso-data-product checkout is not pinned to a commit"
    )
    assert wf.index("make witness") < wf.index("scripts/assert_snapshot.py"), (
        "the numbers are asserted before the run that produces them"
    )

    witness = (ROOT / "scripts" / "witness.py").read_text(encoding="utf-8")
    name = re.search(r'^SNAPSHOT = "([^"]+)"', witness, re.M)
    assert name, "witness.py no longer names the file it fetches"
    assert f"{name.group(1)}\n" in wf, (
        f"witness.py writes {name.group(1)} and the acceptance run reads "
        f"something else"
    )
