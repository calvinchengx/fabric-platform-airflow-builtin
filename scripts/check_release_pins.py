"""The images packaged BY a release must name the release that packaged them.

WHAT THIS PLATFORM PINS. `emulator-sail` and `emulator-spark-agent` are built
and published by a fabric-emulator RELEASE, but they carry their own upstream
versions -- Sail's own version, and the Spark the agent speaks. versions.env
therefore holds two different facts per image:

    SAIL_ENGINE_VERSION=0.7.0     <- the underlying Sail
    SAIL_ENGINE_RELEASE=0.35.0    <- the fabric-emulator release that packaged it

WHY THAT PAIR NEEDS A GUARD. Bumping FABRIC_EMULATOR_VERSION is one edit and
the _RELEASE fields are two more, in a file where every other line is already
correct. Miss them and the stack runs a NEWER emulator against a sail and a
spark-agent packaged for an OLDER one -- with the digests still valid, the
compose file still resolving and every service still starting. Nothing goes
red; the components simply are not the set that was tested together.

WHY THIS PLATFORM HAS NO check_product_pin.py. Its product installs no client
wheel from a release at all -- bronze is delta-rs, silver is dbt-fabricspark,
and both speak Livy/TDS/REST. There is no client/image pair here to check, so
that guard would fail on a condition this repository can never satisfy. This
is the invariant this shape actually has.

A DIGEST WITHOUT A VERSION is the other half. Every image is pinned
`${X_VERSION}@${X_DIGEST}`, and the digest is what identity rests on; a digest
whose version key has been renamed or removed leaves compose interpolating an
empty tag against a pinned hash.

Stdlib only, so it runs before anything is built.
"""

from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
ANCHOR = "FABRIC_EMULATOR_VERSION"


def pins(text: str) -> dict[str, str]:
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def problems(v: dict[str, str]) -> list[str]:
    out = []
    anchor = v.get(ANCHOR)
    if not anchor:
        return [f"versions.env has no {ANCHOR}, so nothing here can be checked "
                f"against the release this platform runs"]

    releases = sorted(k for k in v if k.endswith("_RELEASE"))
    if not releases:
        out.append(
            "versions.env declares no *_RELEASE key. The sail and spark-agent "
            "images are packaged by a fabric-emulator release and must say "
            "which one; if this platform genuinely runs neither, delete this "
            "check rather than leaving it passing on an empty set"
        )
    for k in releases:
        if v[k] != anchor:
            out.append(
                f"{k}={v[k]} but {ANCHOR}={anchor}. That image was packaged by "
                f"a different release than the emulator this platform runs, so "
                f"the components are not the set that was tested together"
            )

    for k in sorted(v):
        if k.endswith("_DIGEST"):
            want = k[: -len("_DIGEST")] + "_VERSION"
            if want not in v:
                out.append(
                    f"{k} has no {want}. Compose pins every image as "
                    f"${{{want}}}@${{{k}}}, so a digest whose version key is "
                    f"missing interpolates an empty tag against a pinned hash"
                )
    return out


def main() -> int:
    path = ROOT / "versions.env"
    if not path.is_file():
        print(f"{path} does not exist", file=sys.stderr)
        return 1
    v = pins(path.read_text(encoding="utf-8"))
    found = problems(v)
    if found:
        print(
            "THE PINNED IMAGES ARE NOT ONE SET.\n\n" + "\n\n".join(found)
            + "\n\nFix versions.env so every *_RELEASE names the same "
              "fabric-emulator release this platform runs.",
            file=sys.stderr,
        )
        return 1
    rel = sorted(k for k in v if k.endswith("_RELEASE"))
    print(f"images: {len(rel)} packaged image(s) name release "
          f"v{v[ANCHOR]} ({', '.join(rel)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
