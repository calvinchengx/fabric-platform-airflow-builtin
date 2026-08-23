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

THREE PINS MOVE, not one. `emulator-sail` and `emulator-spark-agent` are tagged
for the dependency they carry -- 0.7.0 is the Sail engine, 4.2.0 the Spark
Connect client -- and a fabric-emulator release republishes those same tags over
different first-party code. So their VERSIONS stay put while their DIGESTS move,
which is the case a version-only rewrite cannot express at all.

Usage:  python3 scripts/set_release.py 0.32.0
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
VERSIONS = ROOT / "versions.env"

# ONLY the emulator moves with a fabric-emulator tag. Airflow, Postgres, Redis
# and Sail are pinned on their own cadences, and sweeping them along with a
# release that says nothing about them would be a change nobody asked for.
TRACKS_THE_RELEASE = ("FABRIC_EMULATOR_VERSION",)

# digest var prefix -> (image, the tag to resolve; "release" means this release)
PINS = {
    "FABRIC_EMULATOR": ("ghcr.io/calvinchengx/fabric-emulator", "release"),
    "SAIL_ENGINE": ("ghcr.io/calvinchengx/emulator-sail", "SAIL_ENGINE_VERSION"),
    "SPARK_CLIENT": ("ghcr.io/calvinchengx/emulator-spark-agent", "SPARK_CLIENT_VERSION"),
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


def set_digests(text: str, release: str) -> tuple[str, dict[str, tuple[str, str]]]:
    """Rewrite every _DIGEST to what its tag resolves to now."""
    moved = {}
    for prefix, (image, tag_source) in PINS.items():
        found = re.search(rf"^{prefix}_DIGEST=(.*)$", text, re.M)
        if not found:
            raise SystemExit(f"{prefix}_DIGEST not found in versions.env")
        if tag_source == "release":
            tag = release
        else:
            tag_line = re.search(rf"^{tag_source}=(.+)$", text, re.M)
            if not tag_line:
                raise SystemExit(f"{tag_source} not found in versions.env")
            tag = tag_line.group(1).strip()
        digest = digest_of(image, tag)
        moved[prefix] = (found.group(1).strip(), digest)
        text = re.sub(rf"^{prefix}_DIGEST=.*$", f"{prefix}_DIGEST={digest}",
                      text, flags=re.M)
    return text, moved
SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.\-]+)?$")


def set_version(text: str, version: str) -> tuple[str, dict[str, str]]:
    moved: dict[str, str] = {}
    lines = text.splitlines(keepends=True)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, old = stripped.partition("=")
        key, old = key.strip(), old.strip()
        if key in TRACKS_THE_RELEASE:
            moved[key] = old
            lines[i] = f"{key}={version}\n"
    return "".join(lines), moved


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit("usage: set_release.py <version>   e.g. set_release.py 0.32.0")
    version = sys.argv[1].lstrip("v")
    if not SEMVER.match(version):
        sys.exit(f"not a version: {version!r}, expected something like 0.32.0")

    text = VERSIONS.read_text(encoding="utf-8")
    new, moved = set_version(text, version)
    missing = [key for key in TRACKS_THE_RELEASE if key not in moved]
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
        note = "  (unchanged)" if old == version else ""
        print(f"  {key}: {old} -> {version}{note}")
    for prefix, (before, after) in digests.items():
        note = "  (unchanged)" if before == after else ""
        print(f"  {prefix}_DIGEST: {before[:19]}… -> {after[:19]}…{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
