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

    SAIL_ENGINE and SPARK_CLIENT move their digest and _RELEASE on every
    release, and their _VERSION to whatever dependency that release carries.
    """
    import sys as _sys

    _sys.path.insert(0, str(ROOT / "scripts"))
    import set_release

    versions = tmp_path / "versions.env"
    versions.write_text((ROOT / "versions.env").read_text(encoding="utf-8"),
                        encoding="utf-8")
    fake = "sha256:" + "a" * 64
    resolved = []
    # A release that bumped both dependencies, with pysail pinned twice the way
    # fabric-emulator's pyproject.toml really does.
    pyproject = ('dependencies = ["pysail==8.8.8", "pyspark-client==7.7.7"]\n'
                 'engine = ["pysail==8.8.8"]\n')
    saved = (set_release.VERSIONS, set_release.digest_of, set_release.fetch, _sys.argv)
    try:
        set_release.VERSIONS = versions
        set_release.digest_of = lambda image, tag: resolved.append((image, tag)) or fake
        set_release.fetch = lambda url: pyproject if "/v9.9.9/" in url else ""
        _sys.argv = ["set_release.py", "9.9.9"]
        assert set_release.main() == 0
    finally:
        (set_release.VERSIONS, set_release.digest_of,
         set_release.fetch, _sys.argv) = saved

    written = versions.read_text(encoding="utf-8")
    assert re.search(r"^FABRIC_EMULATOR_VERSION=9\.9\.9$", written, re.M)
    for prefix in set_release.PINS:
        assert re.search(rf"^{prefix}_DIGEST={fake}$", written, re.M), (
            f"{prefix} kept a stale digest beside a moved release")
    # EVERY digest comes from the release's own tag. Resolving the sidecars by
    # their dependency tag is the bug this replaces: after v0.36.0 moved pysail
    # to 0.7.1, `emulator-sail:0.7.0` still named v0.35.0's build, and that is
    # what got pinned under a 0.36.0 label.
    assert {tag for _, tag in resolved} == {"9.9.9"}, resolved
    # The dependency versions follow the RELEASE's pins, never the emulator's
    # number, and never a literal in this test. This used to assert they did
    # not move at all, which is the belief v0.36.0 disproved.
    assert re.search(r"^SAIL_ENGINE_VERSION=8\.8\.8$", written, re.M)
    assert re.search(r"^SPARK_CLIENT_VERSION=7\.7\.7$", written, re.M)
    for prefix in ("SAIL_ENGINE", "SPARK_CLIENT"):
        assert re.search(rf"^{prefix}_RELEASE=9\.9\.9$", written, re.M), prefix


def test_a_release_with_an_ambiguous_dependency_pin_writes_nothing(tmp_path):
    """Two different pysail pins means the release cannot say which Sail it
    shipped. Guessing would pin one of them under a label it may not match."""
    import sys as _sys

    import pytest

    _sys.path.insert(0, str(ROOT / "scripts"))
    import set_release

    versions = tmp_path / "versions.env"
    original = (ROOT / "versions.env").read_text(encoding="utf-8")
    versions.write_text(original, encoding="utf-8")
    pyproject = '"pysail==0.7.0"\n"pysail==0.7.1"\n"pyspark-client==4.2.0"\n'
    saved = (set_release.VERSIONS, set_release.digest_of, set_release.fetch, _sys.argv)
    try:
        set_release.VERSIONS = versions
        set_release.digest_of = lambda image, tag: "sha256:" + "b" * 64
        set_release.fetch = lambda url: pyproject
        _sys.argv = ["set_release.py", "9.9.9"]
        with pytest.raises(SystemExit, match="pysail"):
            set_release.main()
    finally:
        (set_release.VERSIONS, set_release.digest_of,
         set_release.fetch, _sys.argv) = saved
    assert versions.read_text(encoding="utf-8") == original


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
