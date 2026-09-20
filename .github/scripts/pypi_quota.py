#!/usr/bin/env python3
"""Pre-flight PyPI storage check for ``sase-core-rs`` uploads.

PyPI caps a project's total size and rejects the first file that does not fit, mid-
upload: that is how 0.34.48 was left with 3 of its 5 files. This script asks the
question *before* the first byte is sent, so an upload that cannot fit fails with the
numbers and the remedy instead of leaving another partial release behind.

The limit is not defined here. ``release-plz.yml`` names it once, in the
workflow-level ``PYPI_PROJECT_LIMIT_BYTES`` variable, so if PyPI grants a size
increase exactly one value changes.

``check DIR``
    Compare ``current project size + the bytes of DIR that PyPI does not already
    hold`` to the limit. Exits 1 (with the numbers and the remedy on stderr) when the
    upload would overflow. Otherwise appends the headroom to ``$GITHUB_STEP_SUMMARY``
    and exits 0. Files PyPI already has are not counted as incoming, because the
    publish step passes ``skip-existing`` and will not upload them again, which keeps a
    heal of a partial release from being refused for bytes it will never send.

    A PyPI API error is logged and exits 0: the guard exists to give a better error,
    and it must not itself become a new way for a good release to fail.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PACKAGE = "sase-core-rs"

LIMIT_ENV = "PYPI_PROJECT_LIMIT_BYTES"
SUMMARY_ENV = "GITHUB_STEP_SUMMARY"

# Number of newest releases averaged to estimate the size of the next release.
RECENT_RELEASES = 10

RUNBOOK = "docs/pypi-retention.md"


@dataclass(frozen=True)
class Quota:
    """Project size now, what an upload would add, and the limit."""

    limit: int
    used: int
    incoming: int
    average_release: int

    @property
    def projected(self) -> int:
        return self.used + self.incoming

    @property
    def overflow(self) -> int:
        return max(self.projected - self.limit, 0)

    @property
    def free_after(self) -> int:
        return self.limit - self.projected

    @property
    def releases_remaining(self) -> int | None:
        """Whole releases that still fit after this upload, at the recent average."""
        if self.average_release <= 0:
            return None
        return max(self.free_after, 0) // self.average_release


def limit_from_env() -> int:
    text = os.environ.get(LIMIT_ENV)
    if text is None:
        raise SystemExit(f"{LIMIT_ENV} is not set; it is defined in release-plz.yml")
    try:
        limit = int(text)
    except ValueError:
        raise SystemExit(f"{LIMIT_ENV} must be an integer byte count, got {text!r}")
    if limit <= 0:
        raise SystemExit(f"{LIMIT_ENV} must be positive, got {limit}")
    return limit


def fetch_project() -> dict[str, Any]:
    url = f"https://pypi.org/pypi/{PACKAGE}/json"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "sase-release-plz"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def release_sizes(project: dict[str, Any]) -> list[tuple[str, int]]:
    """``(version, bytes)`` for every release that holds files, in PyPI's order."""
    return [
        (version, sum(int(f["size"]) for f in files))
        for version, files in project["releases"].items()
        if files
    ]


def published_filenames(project: dict[str, Any]) -> set[str]:
    return {f["filename"] for files in project["releases"].values() for f in files}


def measure(project: dict[str, Any], dist: dict[str, int], limit: int) -> Quota:
    """Quota for uploading ``dist`` (file name -> bytes) into ``project``."""
    sizes = release_sizes(project)
    already = published_filenames(project)
    recent = [size for _, size in sizes[-RECENT_RELEASES:]]
    return Quota(
        limit=limit,
        used=sum(size for _, size in sizes),
        incoming=sum(size for name, size in dist.items() if name not in already),
        average_release=sum(recent) // len(recent) if recent else 0,
    )


def mib(size: int) -> str:
    return f"{size / 1024**2:,.1f} MiB"


def gib(size: int) -> str:
    return f"{size / 1024**3:.2f} GiB"


def overflow_message(quota: Quota) -> str:
    return (
        f"PyPI project '{PACKAGE}' cannot fit this upload; refusing to upload anything "
        f"so no partial release is left behind.\n"
        f"  current project size: {quota.used:,} bytes ({gib(quota.used)})\n"
        f"  incoming upload:      {quota.incoming:,} bytes ({mib(quota.incoming)})\n"
        f"  limit:                {quota.limit:,} bytes ({gib(quota.limit)})\n"
        f"  overflow:             {quota.overflow:,} bytes ({mib(quota.overflow)})\n"
        f"Reclaim storage by deleting old releases (a human-gated, irreversible step; "
        f"see {RUNBOOK}), then re-run this workflow; the publish gate will retry the "
        f"upload."
    )


def summary_markdown(quota: Quota, version: str | None) -> str:
    remaining = quota.releases_remaining
    lines = [
        f"### PyPI storage headroom (`{PACKAGE}`)",
        "",
        "| | Bytes | Size |",
        "| --- | ---: | ---: |",
        f"| Limit | {quota.limit:,} | {gib(quota.limit)} |",
        f"| Used before upload | {quota.used:,} | {gib(quota.used)} |",
        f"| Incoming upload | {quota.incoming:,} | {mib(quota.incoming)} |",
        f"| Used after upload | {quota.projected:,} | {gib(quota.projected)} |",
        f"| Free after upload | {quota.free_after:,} | {gib(quota.free_after)} |",
        "",
        f"{quota.projected / quota.limit:.1%} of the limit used after this upload"
        + (f" ({version})." if version else "."),
    ]
    if remaining is not None:
        lines.append(
            f"About **{remaining}** more releases fit at the recent average of "
            f"{mib(quota.average_release)} per release (newest {RECENT_RELEASES})."
        )
    lines.append(f"Reclaiming storage: `{RUNBOOK}`.")
    return "\n".join(lines) + "\n"


def write_summary(text: str) -> None:
    path = os.environ.get(SUMMARY_ENV)
    if not path:
        print(text)
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(text + "\n")


def cmd_check(args: argparse.Namespace) -> int:
    limit = limit_from_env()
    dist = {p.name: p.stat().st_size for p in sorted(args.dir.iterdir()) if p.is_file()}
    if not dist:
        raise SystemExit(f"{args.dir} contains no files")

    try:
        project = fetch_project()
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, KeyError) as exc:
        # Never let the guard be the reason a good release fails.
        print(
            f"::warning::PyPI quota pre-flight skipped: could not read project size "
            f"({exc}); continuing to upload",
            file=sys.stderr,
        )
        return 0

    quota = measure(project, dist, limit)
    if quota.overflow:
        print(f"::error::{overflow_message(quota)}", file=sys.stderr)
        write_summary(
            summary_markdown(quota, args.version)
            + f"\n**Upload refused: over the limit by {mib(quota.overflow)}.**\n"
        )
        return 1

    print(
        f"PyPI quota ok: {quota.used:,} used + {quota.incoming:,} incoming "
        f"<= {quota.limit:,} limit ({quota.free_after:,} bytes free after upload)"
    )
    write_summary(summary_markdown(quota, args.version))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="fail if the upload in DIR would overflow")
    check.add_argument("dir", type=Path)
    check.add_argument("--version", help="version being published, for the summary")
    check.set_defaults(func=cmd_check)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
