"""Stand up whatever vendors a sources repo declares.

THE PLATFORM OWNS THE MECHANISM, THE DECLARATION OWNS THE CONTENT. This file
knows how to run an OpenAPI simulator and a CDC stack; it does not know that
Contoso exists, how many vendors there are, or what any of them serve. Point it
at a different `sources.yaml` and it stands up those vendors instead.

WHY THIS EXISTS RATHER THAN A HAND-WRITTEN COMPOSE BLOCK, which is what this
platform had: a copy drifts, and this one already had. Three separate facts
were wrong in the copy while the declaration was right --

  * every vendor got `GOMEMLIMIT: 2GiB` and `mem_limit: 4g`, because the block
    was written once for the POS system and pasted. The declaration sizes each
    vendor to ITSELF: 2GiB/4g for the 170 MB exporter, 1GiB/2g for the 36 MB
    one, 256MiB/512m for the one that serves four kilobytes. The copy handed a
    4 KB vendor a 4 GB ceiling.
  * `restart: unless-stopped` existed ONLY here, so a fact about how mokapi
    behaves lived in one platform and not in the vendor that exhibits it, nor
    in the two sibling platforms running that same vendor.
  * the ERP's password, database name and port were restated in three places.

None of that is tidiness. Both this platform and its siblings consume
`contoso-sources`, so all of them pull the same vendor bytes from the same
pinned simulator -- and gold agreeing across four runtimes means something only
if the inputs were identical. A hand-written vendor block makes this platform's
data ITS OWN, and the family comparison starts measuring fixtures instead of
runtimes.

EXPOSED, NOT PUBLISHED. The consumer here is Fabric's built-in Airflow, a
container on this compose network -- unlike the Databricks platform, whose
ingest steps run on the operator's host and therefore need published ports.
Publishing them anyway would invite a second stack on the same host to collide
for no gain.

Emits a compose fragment on stdout rather than starting anything itself, so the
services join the same project, network and lifecycle as the rest of the stack
and `make down` really does take everything with it.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys

# The service that consumes these vendors. The fragment adds the vendors'
# addresses to it, so the ports, hostnames and credentials the DAG reads come
# from the declaration too -- restating them in the platform's compose is the
# same mistake as restating the services, one indirection later.
CONSUMER = "airflow"


def _load(path: pathlib.Path) -> dict:
    """Read sources.yaml without a YAML dependency.

    This platform's image is Fabric's own Airflow sidecar plus the product's
    dependencies; adding PyYAML would mean the platform has opinions about that
    image. The declaration is a small, flat document, so a minimal reader is
    cheaper than the coupling -- and it FAILS on anything it does not
    understand rather than guessing, because a silently skipped vendor surfaces
    much later as an empty landing directory.
    """
    vendors: list[dict] = []
    current: dict | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip() or line.strip() == "vendors:" or line.startswith("version:"):
            continue
        stripped = line.strip()
        if stripped.startswith("- "):
            current = {}
            vendors.append(current)
            stripped = stripped[2:]
        if current is None or ":" not in stripped:
            continue
        key, _, value = stripped.partition(":")
        value = value.strip()
        if value.startswith("[") and value.endswith("]"):
            value = [v.strip() for v in value[1:-1].split(",") if v.strip()]
        current[key.strip()] = value
    return {"vendors": vendors}


def _openapi(name: str, v: dict, sources_dir: str, pins: dict) -> dict:
    service = {
        "image": f"mokapi/mokapi:{pins['MOKAPI_VERSION']}",
        # The dashboard retains every request AND its response body. For a
        # 170 MB export that is a multi-hundred-MB copy per call, so the
        # history is capped at one entry per API.
        "command": ["--event-store-default-size=1",
                    f"/sources/{v['spec']}", f"/sources/{v['script']}"],
        # Go does not read the cgroup limit; without GOMEMLIMIT the heap climbs
        # past mem_limit and the container dies mid-response. THE VALUE IS THE
        # VENDOR'S, not this platform's -- see the module docstring for what
        # happened when it was a constant here.
        "environment": {"GOMEMLIMIT": v["memory"]},
        "mem_limit": v["mem_limit"],
        "volumes": [f"{sources_dir}:/sources:ro"],
        "expose": [str(v["port"])],
        # HEALTHY MEANS THE VENDOR ENFORCES ITS CREDENTIAL, not that a port is
        # open. Without its fixture mokapi does not fail: it GENERATES bodies
        # from the OpenAPI schema and answers every request 200, wrong key
        # included. A probe against `/` cannot tell that apart from a real
        # vendor, and reporting "healthy" for one serving invented data is
        # worse than reporting nothing.
        #
        # PROBED WITH A KEY THAT IS DELIBERATELY WRONG, so the check needs no
        # secret and the platform still learns the vendor is enforcing. wget
        # exits non-zero on 401, which is the healthy case -- hence the
        # inversion. The route comes from the declaration, because which path
        # enforces a credential is a fact about the vendor's API.
        "healthcheck": {
            "test": ["CMD-SHELL",
                     "wget -q -O /dev/null --header='X-Api-Key: definitely-not-the-key' "
                     f"http://localhost:{v['port']}{v['health']} && exit 1 || exit 0"],
            "interval": "10s", "timeout": "5s", "retries": 5,
        } if v.get("health") else {
            "test": ["CMD-SHELL",
                     f"wget -q -O /dev/null http://localhost:{v['port']}/ || test $? -ne 4"],
            "interval": "10s", "timeout": "5s", "retries": 5,
        },
    }
    # OPTIONAL, AND THE VENDOR'S CALL. A restart policy is a statement about
    # how this vendor behaves over a long run, so it belongs beside the memory
    # budget rather than in whichever platform last got bitten.
    if v.get("restart"):
        service["restart"] = v["restart"]
    return service


def _cdc(name: str, v: dict, sources_dir: str, pins: dict, interpreter: str) -> dict:
    """THREE SERVICES, because a change stream needs all three.

    Any two of them is a snapshot wearing a stream's name: the database holds
    the rows, Debezium reads its write-ahead log, and the broker carries what
    Debezium produced. Standing up only Postgres would serve rows -- possibly
    even the right count -- while testing something else entirely.
    """
    db, broker, connect = f"{name}-db", f"{name}-broker", f"{name}-connect"
    # NO DEFAULTS FOR ANY OF THESE. A default is this platform deciding what
    # the vendor's credentials are, and the first draft of this file defaulted
    # the user to a product name -- which the platform's own "carries no
    # product" test caught. A declaration that omits them is a broken
    # declaration, and saying so beats standing up a vendor nobody asked for.
    missing = [k for k in ("db_user", "db_password", "db_name") if not v.get(k)]
    if missing:
        raise SystemExit(
            f"platform: vendor {v['name']!r} is kind=cdc but declares no "
            f"{', '.join(missing)}; this platform will not invent credentials.")
    user, password, database = v["db_user"], v["db_password"], v["db_name"]
    services = {
        db: {
            "image": f"postgres:{pins['POSTGRES_VERSION']}",
            # LOGICAL replication, and the slots to hold it. Debezium reads the
            # WAL; at the default `replica` level there is nothing in it for a
            # decoder to read and the connector attaches to silence.
            "command": ["postgres", "-c", "wal_level=logical",
                        "-c", "max_replication_slots=4", "-c", "max_wal_senders=4"],
            "environment": {"POSTGRES_USER": user,
                            "POSTGRES_PASSWORD": password,
                            "POSTGRES_DB": database},
            "healthcheck": {
                "test": ["CMD-SHELL", f"pg_isready -U {user} -d {database}"],
                "interval": "5s", "timeout": "3s", "retries": 20},
            "volumes": [f"{sources_dir}:/sources:ro"],
        },
        broker: {
            "image": f"docker.redpanda.com/redpandadata/redpanda:{pins['REDPANDA_VERSION']}",
            "command": ["redpanda", "start", "--mode=dev-container", "--smp=1",
                        "--kafka-addr=INTERNAL://0.0.0.0:9092",
                        f"--advertise-kafka-addr=INTERNAL://{broker}:9092"],
            "healthcheck": {"test": ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"],
                            "interval": "5s", "timeout": "5s", "retries": 30},
        },
        connect: {
            "image": f"debezium/connect:{pins['DEBEZIUM_VERSION']}",
            "depends_on": {db: {"condition": "service_healthy"},
                           broker: {"condition": "service_healthy"}},
            "environment": {
                "BOOTSTRAP_SERVERS": f"{broker}:9092",
                "GROUP_ID": v["name"],
                "CONFIG_STORAGE_TOPIC": "_connect_configs",
                "OFFSET_STORAGE_TOPIC": "_connect_offsets",
                "STATUS_STORAGE_TOPIC": "_connect_status",
                # One partition each: ordering per key is what CDC guarantees,
                # and more would trade it away for throughput nothing needs.
                "CONFIG_STORAGE_REPLICATION_FACTOR": "1",
                "OFFSET_STORAGE_REPLICATION_FACTOR": "1",
                "STATUS_STORAGE_REPLICATION_FACTOR": "1"},
            "healthcheck": {"test": ["CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"],
                            "interval": "10s", "timeout": "5s", "retries": 30},
        },
    }
    if v.get("seed"):
        # THE SEEDER MAKES THE VENDOR EXIST. It registers the connector and then
        # replays the fixture's history as real INSERT/UPDATE/DELETE, so the
        # stream is CAPTURED rather than described. Landing a file that
        # describes a change feed would be a different and weaker claim.
        #
        # A step, not a service: `restart: no` and a one-shot command, because
        # it must not loop if the replay fails. It runs the VENDOR'S OWN
        # scripts -- this platform supplies an interpreter and nothing else.
        services[f"{name}-seed"] = {
            "image": f"python:{interpreter}-slim",
            "depends_on": {db: {"condition": "service_healthy"},
                           connect: {"condition": "service_healthy"}},
            "environment": {
                "ERP_DSN": (f"host={db} port=5432 dbname={database} "
                            f"user={user} password={password}"),
                "ERP_CONNECT_URL": f"http://{connect}:8083",
                "ERP_DB_HOST": db,
                "PYTHONUNBUFFERED": "1",
            },
            "volumes": [f"{sources_dir}:/sources:rw"],
            "working_dir": "/sources",
            # ONE INTERPRETER throughout, and `--frozen --no-sync` is
            # load-bearing. A bare `uv run` RE-SYNCS and prunes anything absent
            # from the lock, evicting the generators and psycopg installed
            # moments earlier and then failing with ModuleNotFoundError for a
            # package that was just there. psycopg goes in AFTER fixtures.py,
            # because that script calls `uv sync` itself.
            "command": ["sh", "-c",
                        "pip install --quiet uv && "
                        "uv sync --quiet && "
                        f"uv run --frozen --no-sync python {v['seed'].rsplit('/', 1)[0]}/fixtures.py && "
                        "uv pip install --quiet 'psycopg[binary]' && "
                        f"uv run --frozen --no-sync python {v['seed']}"],
            "restart": "no",
        }
    return services


def _consumer_env(decl: dict, root: pathlib.Path) -> dict:
    """Where the DAG finds each vendor, derived rather than restated.

    The product reads these by name. In PRODUCTION none of it is instantiated:
    the vendors are real and the same names carry their real addresses, which
    is why the names are the seam and the values are not.
    """
    env: dict[str, str] = {}
    for v in decl["vendors"]:
        name = v["name"].replace("_", "-")
        stem = v["name"].upper()
        if v.get("kind") == "openapi":
            env[f"{stem}_API"] = f"http://{name}:{v['port']}"
            # THE VENDOR'S OWN CREDENTIAL, from its fixture directory. Each
            # vendor's key rotates separately -- that is what having more than
            # one vendor means -- and a platform that asserted a key would be
            # deciding what the vendor is. This platform once did exactly that
            # in a sibling and shipped `pos-dev-key` against a vendor whose key
            # was `pos-key-8843-dev`.
            key_file = root / v["data"] / ".api-key"
            if not key_file.exists():
                raise SystemExit(
                    f"platform: vendor {v['name']!r} declares data={v['data']!r} but "
                    f"{key_file} does not exist -- run the sources repo's fixture "
                    f"step first; guessing the key would authenticate against nothing.")
            env[f"{stem}_API_KEY"] = key_file.read_text().strip()
        elif v.get("kind") == "cdc":
            env[f"{stem}_BROKER"] = f"{name}-broker:9092"
            env[f"{stem}_TOPIC"] = v["topic"]
            env[f"{stem}_DSN"] = (
                f"host={name}-db port=5432 dbname={v['db_name']} "
                f"user={v['db_user']} password={v['db_password']}")
    return env


def fragment(decl: dict, sources_dir: str, pins: dict, root: pathlib.Path,
             interpreter: str) -> dict:
    services: dict = {}
    for v in decl["vendors"]:
        name = v["name"].replace("_", "-")
        kind = v.get("kind")
        if kind == "openapi":
            services[name] = _openapi(name, v, sources_dir, pins)
        elif kind == "cdc":
            services.update(_cdc(name, v, sources_dir, pins, interpreter))
        else:
            raise SystemExit(
                f"platform: vendor {v['name']!r} declares kind={kind!r}, which this "
                f"platform does not know how to run. Add it here or fix the "
                f"declaration; guessing would stand up the wrong vendor.")
    services[CONSUMER] = {"environment": _consumer_env(decl, root)}
    return {"services": services}


def main() -> int:
    if len(sys.argv) != 3:
        sys.exit("usage: sources.py <path-to-sources.yaml> <sources-dir-abs>")
    decl = _load(pathlib.Path(sys.argv[1]))
    if not decl["vendors"]:
        sys.exit("platform: that sources.yaml declares no vendors")
    root = pathlib.Path(sys.argv[2])
    # EVERY IMAGE THIS PLATFORM STARTS ON A PRODUCT'S BEHALF IS PINNED BY THE
    # SOURCES REPO. A platform defaulting any of them would be deciding what
    # the vendor is, and a guessed tag fails at pull time with `manifest
    # unknown` -- which is how the sibling's version of this check came to be.
    versions = root / "versions.env"
    pins = {}
    if versions.exists():
        pins = {k.strip(): val.strip() for k, val in (
            line.split("=", 1) for line in versions.read_text().splitlines()
            if "=" in line and not line.strip().startswith("#"))}
    needed = {"openapi": ["MOKAPI_VERSION"],
              "cdc": ["POSTGRES_VERSION", "REDPANDA_VERSION", "DEBEZIUM_VERSION"]}
    for v in decl["vendors"]:
        for key in needed.get(v.get("kind"), []):
            if key not in pins:
                sys.exit(f"platform: vendor {v['name']!r} is kind={v.get('kind')!r} but "
                         f"{versions} does not pin {key}; this platform will not guess it")
    # THE INTERPRETER IS THE PLATFORM'S, the vendor images are the vendor's.
    # `python:3.12-slim` is how this platform chooses to run a script the
    # vendor wrote; mokapi, postgres, redpanda and debezium are what the vendor
    # IS. That is the whole line between mechanism and content, and it decides
    # which versions.env each pin belongs in.
    interpreter = os.environ.get("PYTHON_VERSION", "")
    if not interpreter:
        sys.exit("platform: PYTHON_VERSION is unset -- it pins the interpreter this "
                 "platform runs a vendor's seed script with, and is this platform's "
                 "to choose. See versions.env.")
    print(json.dumps(fragment(decl, sys.argv[2], pins, root, interpreter), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
