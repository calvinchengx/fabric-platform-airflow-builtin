#!/usr/bin/env python3
"""Point versions.env at a fabric-emulator release.

WHY A SCRIPT AND NOT A `sed` IN THE WORKFLOW. The acceptance run has to verify
the release that fired rather than whatever this checkout pins, so something
must rewrite the pin before the stack starts. Putting that in the workflow
would hide it from every local user and from review, and the two sibling
platforms already keep it here, where it can be read and run by hand.

IT MOVES THE DIGESTS TOO, and it has to. This platform used to pin tags only,
and the note here said a digest lookup would be machinery for a field that does
not exist. Digests were added, and that turned the omission into a silent
downgrade: docker IGNORES the tag in `repo:tag@sha256:...` and fetches the
digest, so a release run that wrote `FABRIC_EMULATOR_VERSION=0.33.0` beside
0.32.0's digest would have started the PREVIOUS emulator and reported the
acceptance run as verifying the new one.

THE SIDECARS MOVE THREE FIELDS. `emulator-sail` and `emulator-spark-agent` are
tagged for the dependency they carry (pysail, pyspark-client), and a
fabric-emulator release republishes those tags over different first-party code.
So their DIGEST and their _RELEASE label move on every release, and their
_VERSION moves whenever the release bumped the dependency.

This script used to hold _VERSION still, on the belief that a release never
changes which Sail it ships. v0.36.0 moved pysail 0.7.0 -> 0.7.1, and this
script then resolved the digest from the old `emulator-sail:0.7.0` tag: the
previous release's engine, pinned under the new release's label. The version
is now read from the release itself, and the digest from the release's own tag.

Usage:  python3 scripts/set_release.py 0.32.0
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
VERSIONS = ROOT / "versions.env"

# ONLY the emulator and its own sidecars move with a fabric-emulator tag.
# Airflow, Postgres and Redis are pinned on their own cadences, and sweeping
# them along with a release that says nothing about them would be a change
# nobody asked for.
TRACKS_THE_RELEASE = ("FABRIC_EMULATOR_VERSION",)

# The sidecars' TAG names the dependency they carry, so it cannot say which
# release built them. `_RELEASE` says it, and this is what keeps it true: after
# the 0.34.0 bump these labels read 0.33.0 above 0.34.0 digests, because nothing
# here moved them. versions.env asserted something false, and the BOM's gate
# compares exactly this field.
#
# prefix -> the pin in fabric-emulator's pyproject.toml the image is tagged
# with. The same map as fabric-emulator's scripts/image_tags.py, which is what
# chose the tag the release pushed.
TAGGED_BY = {"SAIL_ENGINE": "pysail", "SPARK_CLIENT": "pyspark-client"}
CARRIES_A_DEPENDENCY_TAG = tuple(TAGGED_BY)

# A TAG, not a branch: what the release was built from, and it cannot move.
FABRIC_PYPROJECT = ("https://raw.githubusercontent.com/calvinchengx/"
                    "fabric-emulator/v{release}/pyproject.toml")

# digest var prefix -> image. Every one resolves THIS RELEASE's tag: the
# release pushes `emulator-sail:<release>` beside `emulator-sail:<pysail>`,
# and only the first still names this release once a later one republishes
# the second.
PINS = {
    "FABRIC_EMULATOR": "ghcr.io/calvinchengx/fabric-emulator",
    "SAIL_ENGINE": "ghcr.io/calvinchengx/emulator-sail",
    "SPARK_CLIENT": "ghcr.io/calvinchengx/emulator-spark-agent",
}


def digest_of(image: str, tag: str) -> str:
    """The INDEX digest this tag points at right now.

    The index, not one platform's manifest: pinning `linux/amd64` gives a stack
    that pulls on the CI runner and fails on an arm64 laptop.
    """
    out = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", f"{image}:{tag}",
         "--format", "{{.Manifest.Digest}}"],
        capture_output=True, text=True)
    if out.returncode != 0 or not out.stdout.strip().startswith("sha256:"):
        raise SystemExit(f"cannot read digest for {image}:{tag}: "
                         f"{(out.stderr or out.stdout).strip()[:200]}")
    return out.stdout.strip()


def fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read().decode("utf-8")


def carried_versions(release: str) -> dict[str, str]:
    """The dependency version each sidecar carries in `release`.

    Read with image_tags.py's own rule: exactly one `==` pin per package.
    """
    url = FABRIC_PYPROJECT.format(release=release)
    try:
        text = fetch(url)
    except OSError as err:
        raise SystemExit(f"cannot read {url}: {err}") from None
    carried = {}
    for prefix, package in TAGGED_BY.items():
        found = set(re.findall(rf'"{re.escape(package)}==([0-9][^"]*)"', text))
        if len(found) != 1:
            raise SystemExit(f"v{release} pins {package} as {sorted(found) or 'nothing'}; "
                             f"expected exactly one == version")
        carried[prefix] = found.pop()
    return carried


def set_digests(text: str, release: str) -> tuple[str, dict[str, tuple[str, str]]]:
    """Rewrite every _DIGEST to what this release's tag resolves to now."""
    moved = {}
    for prefix, image in PINS.items():
        found = re.search(rf"^{prefix}_DIGEST=(.*)$", text, re.M)
        if not found:
            raise SystemExit(f"{prefix}_DIGEST not found in versions.env")
        digest = digest_of(image, release)
        moved[prefix] = (found.group(1).strip(), digest)
        text = re.sub(rf"^{prefix}_DIGEST=.*$", f"{prefix}_DIGEST={digest}",
                      text, flags=re.M)
    return text, moved
SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.\-]+)?$")


def set_version(text: str, version: str,
                carried: dict[str, str] | None = None) -> tuple[str, dict[str, str]]:
    """Move the release pins, and each sidecar's _VERSION to what it carries."""
    carried = carried or {}
    moved: dict[str, str] = {}
    lines = text.splitlines(keepends=True)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, old = stripped.partition("=")
        key, old = key.strip(), old.strip()
        prefix, _, field = key.rpartition("_")
        if key in TRACKS_THE_RELEASE:
            moved[key] = old
            lines[i] = f"{key}={version}\n"
        elif prefix in CARRIES_A_DEPENDENCY_TAG and field == "RELEASE":
            moved[key] = old
            lines[i] = f"{key}={version}\n"
        elif prefix in carried and field == "VERSION":
            moved[key] = old
            lines[i] = f"{key}={carried[prefix]}\n"
    return "".join(lines), moved


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit("usage: set_release.py <version>   e.g. set_release.py 0.32.0")
    version = sys.argv[1].lstrip("v")
    if not SEMVER.match(version):
        sys.exit(f"not a version: {version!r}, expected something like 0.32.0")

    carried = carried_versions(version)
    text = VERSIONS.read_text(encoding="utf-8")
    new, moved = set_version(text, version, carried)
    missing = [key for key in TRACKS_THE_RELEASE if key not in moved]
    missing += [f"{prefix}_VERSION" for prefix in carried
                if f"{prefix}_VERSION" not in moved]
    if missing:
        # A pin that vanished is not a no-op: the workflow would go on to verify
        # the old version while reporting the new one.
        sys.exit(f"{VERSIONS.name} has no {', '.join(missing)} to set")
    # Digests BEFORE the write. Resolving can fail — a tag not published yet, a
    # registry that will not answer — and failing after the rewrite would leave
    # versions.env naming a release whose images nobody confirmed exist.
    new, digests = set_digests(new, version)

    VERSIONS.write_text(new, encoding="utf-8")
    for key, old in moved.items():
        now = carried.get(key.removesuffix("_VERSION"), version)
        note = "  (unchanged)" if old == now else ""
        print(f"  {key}: {old} -> {now}{note}")
    for prefix, (before, after) in digests.items():
        note = "  (unchanged)" if before == after else ""
        print(f"  {prefix}_DIGEST: {before[:19]}… -> {after[:19]}…{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
