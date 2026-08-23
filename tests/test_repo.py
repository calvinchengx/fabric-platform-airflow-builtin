"""Repo-boundary tests: what the compose file pins. No Docker, no emulator."""

from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent




# --- digest pins ---------------------------------------------------------------
#
# Docker IGNORES the tag in `repo:tag@sha256:...` — the digest decides, silently.
# A version bumped without its digest runs the OLD image under the NEW name.

def test_every_pullable_image_is_fetched_by_digest():
    """Every `image:` line, except one that says why it cannot be pinned.

    The exemption is required to carry a reason and to sit on the line it
    excuses, rather than in a list somewhere that drifts away from it — the
    convention fabric-emulator's own checker uses.
    """
    compose = (ROOT / "compose" / "docker-compose.yml").read_text(encoding="utf-8")
    lines = compose.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("image:"):
            continue
        window = "\n".join(lines[max(0, i - 3):i])
        if "digest-exempt:" in window:
            assert len(window.split("digest-exempt:")[1].strip()) > 10, (
                f"an exemption with no reason: {stripped}")
            continue
        assert "@${" in stripped and "_DIGEST" in stripped, f"pulled by tag alone: {stripped}"
        assert ":-" not in stripped, f"a default version floats: {stripped}"


def test_every_digest_var_is_a_real_digest():
    text = (ROOT / "versions.env").read_text(encoding="utf-8")
    found = re.findall(r"^([A-Z_]+)_DIGEST=(.*)$", text, re.M)
    assert found, "no digests are pinned at all"
    for prefix, value in found:
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", value), f"{prefix}: {value!r}"
        assert re.search(rf"^{prefix}_VERSION=.+$", text, re.M), (
            f"{prefix}_DIGEST has no {prefix}_VERSION beside it to read")


def test_a_release_moves_every_digest_with_its_version(tmp_path):
    """THE BUG THIS REPLACES: `set_release.py` moved FABRIC_EMULATOR_VERSION and
    left every digest behind, because it predates the digests being added.

    Docker ignores the tag in `repo:tag@sha256:...` and fetches the digest, so
    the next release would have started the PREVIOUS emulator while the
    acceptance run reported it as verifying the new one — a green run for a
    release nobody tested, which is exactly what this script exists to prevent.

    Note SAIL_ENGINE and SPARK_CLIENT: their VERSIONS do not move (0.7.0 is the
    Sail engine, 4.2.0 the Spark Connect client) but their DIGESTS must, because
    a fabric-emulator release republishes those tags over different code.
    """
    import sys as _sys

    _sys.path.insert(0, str(ROOT / "scripts"))
    import set_release

    versions = tmp_path / "versions.env"
    versions.write_text((ROOT / "versions.env").read_text(encoding="utf-8"),
                        encoding="utf-8")
    fake = "sha256:" + "a" * 64
    saved = (set_release.VERSIONS, set_release.digest_of, _sys.argv)
    try:
        set_release.VERSIONS = versions
        set_release.digest_of = lambda image, tag: fake
        _sys.argv = ["set_release.py", "9.9.9"]
        assert set_release.main() == 0
    finally:
        set_release.VERSIONS, set_release.digest_of, _sys.argv = saved

    written = versions.read_text(encoding="utf-8")
    assert re.search(r"^FABRIC_EMULATOR_VERSION=9\.9\.9$", written, re.M)
    for prefix in set_release.PINS:
        assert re.search(rf"^{prefix}_DIGEST={fake}$", written, re.M), (
            f"{prefix} kept a stale digest beside a moved release")
    # The dependency versions must NOT be dragged to the emulator's number.
    assert re.search(r"^SAIL_ENGINE_VERSION=0\.7\.0$", written, re.M)
    assert re.search(r"^SPARK_CLIENT_VERSION=4\.2\.0$", written, re.M)


def test_every_digest_in_versions_env_is_moved_by_a_release():
    """A digest nobody moves is a pin that silently goes stale. Third-party
    images are exempt: they are not retagged by a fabric-emulator release, so
    they move only when a person bumps them."""
    import sys as _sys

    _sys.path.insert(0, str(ROOT / "scripts"))
    from set_release import PINS

    text = (ROOT / "versions.env").read_text(encoding="utf-8")
    family = {m for m in re.findall(r"^([A-Z_]+)_DIGEST=", text, re.M)
              if m in {"FABRIC_EMULATOR", "SAIL_ENGINE", "SPARK_CLIENT"}}
    assert family <= set(PINS), f"not moved on release: {sorted(family - set(PINS))}"
