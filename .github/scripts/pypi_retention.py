#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["packaging"]
# ///
"""Plan and verify PyPI storage retention for ``sase-core-rs``.

PyPI has no delete API, so deletion itself stays a human-gated ``pypi-cleanup``
run (see ``docs/pypi-retention.md``). This script owns everything around it:
deriving the delete list from live data, turning that list into the exact
``--version-regex`` argument, proving a ``pypi-cleanup --query-only`` run selected
the same set, and re-measuring the project afterwards.

Subcommands: ``plan``, ``regex``, ``compare``, ``verify``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from packaging.version import Version


PACKAGE = "sase-core-rs"

# PyPI rejected an upload once the project crossed ~10.73e9 bytes (summing the
# `size` of every file in the JSON API), i.e. 10 GiB, not 10e9 bytes.
PROJECT_LIMIT_BYTES = 10 * 1024**3

DEFAULT_KEEP = 30

# Observed release cadence before the daily-cut change, and the target after it.
CADENCES_PER_DAY = (4.32, 1.0)

# Number of newest releases averaged to estimate the size of the next release.
RECENT_RELEASES = 10

# A complete release: four platform wheels plus the sdist. A release with fewer
# files is a partial upload (0.34.48 lost its win_amd64 wheel and sdist when the
# quota tripped mid-upload).
EXPECTED_FILES_PER_RELEASE = 5

# Header pypi-cleanup logs before listing the versions it selected, one per line.
CLEANUP_HEADER = "Found the following releases of package"
CLEANUP_VERSION_LINE = re.compile(r"^INFO:root: (\S+)\s*$")


@dataclass(frozen=True)
class Release:
    version: Version
    name: str
    files: int
    size: int


def fetch_project(package: str) -> dict[str, Any]:
    url = f"https://pypi.org/pypi/{package}/json"
    request = urllib.request.Request(url, headers={"User-Agent": "sase-pypi-retention"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise SystemExit(f"failed to query {url}: {exc}") from exc


def load_releases(project: dict[str, Any]) -> list[Release]:
    """Published releases, oldest first. Releases with no files do not exist."""
    releases = [
        Release(
            version=Version(name),
            name=name,
            files=len(files),
            size=sum(int(f["size"]) for f in files),
        )
        for name, files in project["releases"].items()
        if files
    ]
    return sorted(releases, key=lambda release: release.version)


def read_list(path: Path) -> list[str]:
    versions = [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if len(versions) != len(set(versions)):
        raise SystemExit(f"{path} lists a version more than once")
    return versions


def version_regex(versions: list[str]) -> str:
    """Anchored regex; pypi-cleanup uses ``re.match``, which only anchors the start."""
    if not versions:
        raise SystemExit("refusing to build a regex from an empty delete list")
    return "^(?:" + "|".join(re.escape(v) for v in versions) + ")$"


def abbreviate(versions: list[str], limit: int = 8) -> str:
    if len(versions) <= limit:
        return str(versions)
    return f"{versions[:limit]} ... and {len(versions) - limit} more"


def gb(size: int) -> str:
    return f"{size / 1e9:.2f} GB"


def report_headroom(releases: list[Release]) -> None:
    used = sum(release.size for release in releases)
    free = PROJECT_LIMIT_BYTES - used
    recent = releases[-RECENT_RELEASES:]
    average = sum(release.size for release in recent) // max(len(recent), 1)
    print(
        f"  used {gb(used)} ({used / PROJECT_LIMIT_BYTES:.1%} of "
        f"{PROJECT_LIMIT_BYTES / 1024**3:.0f} GiB), free {gb(free)}"
    )
    if average <= 0 or free <= 0:
        return
    remaining = free // average
    print(f"  average newest-{len(recent)} release {average / 1e6:.1f} MB -> ~{remaining} more releases")
    for cadence in CADENCES_PER_DAY:
        print(f"    runway at {cadence:g} releases/day: ~{remaining / cadence:.0f} days")


def cmd_plan(args: argparse.Namespace) -> int:
    if args.input_json:
        project = json.loads(Path(args.input_json).read_text())
    else:
        project = fetch_project(args.package)
    releases = load_releases(project)
    published = {release.name for release in releases}

    pinned = set(args.keep_version)
    unknown = pinned - published
    if unknown:
        raise SystemExit(f"--keep-version names unpublished versions: {sorted(unknown)}")

    newest = {release.name for release in releases[-args.keep :]}
    keep = [release for release in releases if release.name in newest | pinned]
    delete = [release for release in releases if release.name not in newest | pinned]
    if not keep:
        raise SystemExit("refusing to plan a deletion that keeps nothing")
    if not delete:
        print("nothing to delete: every published version is inside the keep set")
        return 0

    kept_size = sum(release.size for release in keep)
    freed = sum(release.size for release in delete)
    total = kept_size + freed
    print(
        f"{args.package}: {len(releases)} releases, "
        f"{sum(release.files for release in releases)} files, {gb(total)} now"
    )
    print(f"keep   {len(keep):>3} ({keep[0].name} .. {keep[-1].name}): {gb(kept_size)}")
    if pinned:
        print(f"       explicitly pinned outside the newest {args.keep}: {sorted(pinned)}")
    print(f"delete {len(delete):>3} ({delete[0].name} .. {delete[-1].name}): {gb(freed)}")
    print("after deletion:")
    report_headroom(keep)

    output = Path(args.output)
    output.write_text("".join(f"{release.name}\n" for release in delete))
    print(f"wrote {len(delete)} versions to {output}")
    return 0


def cmd_regex(args: argparse.Namespace) -> int:
    print(version_regex(read_list(Path(args.list))))
    return 0


def parse_cleanup_selection(log: str) -> set[str]:
    """Versions a ``pypi-cleanup`` run reported it would delete."""
    selected: set[str] = set()
    in_block = False
    for line in log.splitlines():
        if CLEANUP_HEADER in line:
            in_block = True
            continue
        if not in_block:
            continue
        match = CLEANUP_VERSION_LINE.match(line)
        if match is None:
            break
        selected.add(match.group(1))
    if not in_block:
        raise SystemExit(
            "pypi-cleanup output has no 'Found the following releases' block; "
            "it selected nothing or its log format changed"
        )
    return selected


def cmd_compare(args: argparse.Namespace) -> int:
    expected = set(read_list(Path(args.list)))
    log = sys.stdin.read() if args.log == "-" else Path(args.log).read_text()
    selected = parse_cleanup_selection(log)
    outside = sorted(selected - expected, key=Version)
    missing = sorted(expected - selected, key=Version)
    if outside:
        print(
            f"MISMATCH: pypi-cleanup selects {len(outside)} versions NOT in the list: {abbreviate(outside)}",
            file=sys.stderr,
        )
        return 1
    if missing and not args.allow_subset:
        print(
            f"MISMATCH: pypi-cleanup does not select {len(missing)} listed versions: {abbreviate(missing)}",
            file=sys.stderr,
        )
        return 1
    note = f" ({len(missing)} listed versions already gone)" if missing else ""
    print(f"OK: pypi-cleanup selects {len(selected)} versions, all in the delete list{note}")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    if args.input_json:
        project = json.loads(Path(args.input_json).read_text())
    else:
        project = fetch_project(args.package)
    releases = load_releases(project)
    published = {release.name for release in releases}
    used = sum(release.size for release in releases)
    files = sum(release.files for release in releases)
    print(f"{args.package}: {len(releases)} releases, {files} files, {gb(used)}")
    report_headroom(releases)
    partial = [release for release in releases if release.files < EXPECTED_FILES_PER_RELEASE]
    if partial:
        print(
            f"  partial releases (fewer than {EXPECTED_FILES_PER_RELEASE} files): "
            + abbreviate([f"{release.name} ({release.files})" for release in partial])
        )

    problems: list[str] = []
    if args.list:
        leftover = sorted(published & set(read_list(Path(args.list))), key=Version)
        if leftover:
            problems.append(
                f"{len(leftover)} versions from the delete list are still published: {abbreviate(leftover)}"
            )
    absent = sorted(set(args.require_version) - published)
    if absent:
        problems.append(f"required versions are not published: {absent}")
    if args.expect_releases is not None and len(releases) != args.expect_releases:
        problems.append(f"expected {args.expect_releases} releases, found {len(releases)}")
    if args.expect_oldest is not None and releases[0].name != args.expect_oldest:
        problems.append(f"expected {args.expect_oldest} to be the oldest release, found {releases[0].name}")
    if used >= PROJECT_LIMIT_BYTES:
        problems.append(f"project is at or over the {PROJECT_LIMIT_BYTES} byte limit")
    for problem in problems:
        print(f"FAIL: {problem}", file=sys.stderr)
    if not problems:
        print("OK")
    return 1 if problems else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--package", default=PACKAGE)
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="derive the delete list from live PyPI data")
    plan.add_argument("--keep", type=int, default=DEFAULT_KEEP, help="newest published versions to keep")
    plan.add_argument(
        "--keep-version",
        action="append",
        default=[],
        help="also keep this version (repeatable), e.g. one a consumer pins exactly",
    )
    plan.add_argument("--output", required=True, help="file to write the delete list to")
    plan.add_argument("--input-json", help="read this saved PyPI JSON response instead of querying PyPI")
    plan.set_defaults(handler=cmd_plan)

    regex = sub.add_parser("regex", help="print the pypi-cleanup --version-regex for a delete list")
    regex.add_argument("--list", required=True)
    regex.set_defaults(handler=cmd_regex)

    compare = sub.add_parser("compare", help="check a pypi-cleanup --query-only log against a delete list")
    compare.add_argument("--list", required=True)
    compare.add_argument("--log", default="-", help="pypi-cleanup output file (default: stdin)")
    compare.add_argument(
        "--allow-subset",
        action="store_true",
        help="tolerate listed versions that are already gone; never tolerates extra ones",
    )
    compare.set_defaults(handler=cmd_compare)

    verify = sub.add_parser("verify", help="re-measure the project and assert the retention outcome")
    verify.add_argument("--list", help="delete list whose versions must all be gone")
    verify.add_argument("--require-version", action="append", default=[], help="version that must remain")
    verify.add_argument("--expect-releases", type=int, help="exact number of releases that must remain")
    verify.add_argument(
        "--expect-oldest",
        help="version that must be the oldest release left; tolerates newer releases published since the deletion",
    )
    verify.add_argument("--input-json", help="read this saved PyPI JSON response instead of querying PyPI")
    verify.set_defaults(handler=cmd_verify)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
