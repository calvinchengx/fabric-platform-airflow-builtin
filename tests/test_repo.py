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
