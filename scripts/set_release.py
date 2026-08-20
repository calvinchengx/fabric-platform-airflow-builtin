#!/usr/bin/env python3
"""Point versions.env at a fabric-emulator release.

WHY A SCRIPT AND NOT A `sed` IN THE WORKFLOW. The acceptance run has to verify
the release that fired rather than whatever this checkout pins, so something
must rewrite the pin before the stack starts. Putting that in the workflow
would hide it from every local user and from review, and the two sibling
platforms already keep it here, where it can be read and run by hand.

DELIBERATELY NARROWER THAN THE AIRFLOW 3 PLATFORM'S. That one also moves image
DIGESTS, because it pins them; this platform pins tags only, so there is nothing
to resolve and a digest lookup here would be machinery for a field that does not
exist. If digests are ever added, that script is the model.

Usage:  python3 scripts/set_release.py 0.32.0
"""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
VERSIONS = ROOT / "versions.env"

# ONLY the emulator moves with a fabric-emulator tag. Airflow, Postgres, Redis
# and Sail are pinned on their own cadences, and sweeping them along with a
# release that says nothing about them would be a change nobody asked for.
TRACKS_THE_RELEASE = ("FABRIC_EMULATOR_VERSION",)
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
    VERSIONS.write_text(new, encoding="utf-8")
    for key, old in moved.items():
        note = "  (unchanged)" if old == version else ""
        print(f"  {key}: {old} -> {version}{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
