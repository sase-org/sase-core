#!/usr/bin/env python3
"""Decide whether a ``sase-core-rs`` release is complete on PyPI.

A version that *exists* on PyPI is not necessarily a version that is *published*:
0.34.48 stopped at 3 of 5 files when the project quota tripped mid-upload, and
"the version answers HTTP 200" read that as done, so the self-healing publish loop
never topped it up. This script asks the stronger question instead: does the
release hold every distribution in the expected set, none of them yanked?

The expected set is not defined here. ``release-plz.yml`` names it once, in the
workflow-level ``EXPECTED_DIST_SUFFIXES`` variable, and this script reads it from the
environment, so the build matrix and the gate have a single place to be updated
together.

Subcommands:

``status VERSION``
    Print ``absent`` (PyPI has no such version), ``partial`` (it exists but the
    expected set is not all present and unyanked), or ``complete`` on stdout, with the
    reasoning on stderr. Exits non-zero only when PyPI cannot be queried.

``dist DIR VERSION``
    Check that ``DIR`` holds exactly one file per expected entry and nothing else, so a
    build-matrix change that the expected set was not updated for fails before upload.
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
from typing import Any, Iterable


PACKAGE = "sase-core-rs"

# Distribution file names are ``sase_core_rs-<version>`` followed by the suffix.
FILENAME_STEM = "sase_core_rs"

SUFFIXES_ENV = "EXPECTED_DIST_SUFFIXES"

ABSENT = "absent"
PARTIAL = "partial"
COMPLETE = "complete"


@dataclass(frozen=True)
class ReleaseFiles:
    """What a release holds, measured against the expected set."""

    state: str
    missing: tuple[str, ...]
    yanked: tuple[str, ...]


def parse_suffixes(text: str) -> list[str]:
    """Expected-set entries from a whitespace-separated list, order preserved."""
    suffixes = text.split()
    if not suffixes:
        raise SystemExit(f"{SUFFIXES_ENV} is empty; refusing to gate on an empty set")
    if len(suffixes) != len(set(suffixes)):
        raise SystemExit(f"{SUFFIXES_ENV} lists an entry more than once: {suffixes}")
    return suffixes


def suffixes_from_env() -> list[str]:
    text = os.environ.get(SUFFIXES_ENV)
    if text is None:
        raise SystemExit(f"{SUFFIXES_ENV} is not set; it is defined in release-plz.yml")
    return parse_suffixes(text)


def stem(version: str) -> str:
    return f"{FILENAME_STEM}-{version}"


def matches(filename: str, version: str, suffix: str) -> bool:
    return filename.startswith(stem(version)) and filename.endswith(suffix)


def classify(
    version: str, files: Iterable[dict[str, Any]], suffixes: list[str]
) -> ReleaseFiles:
    """Compare a release's file list (PyPI JSON ``urls`` entries) to the expected set."""
    files = list(files)
    if not files:
        # PyPI keeps no record of a release with no files, so it is not published.
        return ReleaseFiles(ABSENT, tuple(suffixes), ())

    missing: list[str] = []
    yanked: list[str] = []
    for suffix in suffixes:
        candidates = [f for f in files if matches(f["filename"], version, suffix)]
        if not candidates:
            missing.append(suffix)
        elif all(f.get("yanked") for f in candidates):
            yanked.append(suffix)

    state = COMPLETE if not missing and not yanked else PARTIAL
    return ReleaseFiles(state, tuple(missing), tuple(yanked))


def fetch_release_files(version: str) -> list[dict[str, Any]] | None:
    """Files of one release, or ``None`` when PyPI has no such version."""
    url = f"https://pypi.org/pypi/{PACKAGE}/{version}/json"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "sase-release-plz"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)["urls"]
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def cmd_status(args: argparse.Namespace) -> int:
    suffixes = suffixes_from_env()
    files = fetch_release_files(args.version)
    result = classify(args.version, files or [], suffixes)

    if result.state == PARTIAL:
        for suffix in result.missing:
            print(f"{args.version}: missing *{suffix}", file=sys.stderr)
        for suffix in result.yanked:
            print(f"{args.version}: only yanked files for *{suffix}", file=sys.stderr)
    print(result.state)
    return 0


def dist_problems(
    names: list[str], version: str, suffixes: list[str]
) -> list[str]:
    """Ways ``names`` differ from exactly one file per expected entry."""
    problems: list[str] = []
    for suffix in suffixes:
        found = [n for n in names if matches(n, version, suffix)]
        if not found:
            problems.append(f"no file matches *{suffix}")
        elif len(found) > 1:
            problems.append(f"{len(found)} files match *{suffix}: {sorted(found)}")
    for name in names:
        if not any(matches(name, version, suffix) for suffix in suffixes):
            problems.append(f"{name} matches no expected entry")
    return problems


def cmd_dist(args: argparse.Namespace) -> int:
    suffixes = suffixes_from_env()
    names = sorted(path.name for path in args.dir.iterdir() if path.is_file())
    problems = dist_problems(names, args.version, suffixes)
    if problems:
        print(
            f"{args.dir} does not hold exactly the expected {SUFFIXES_ENV} set "
            f"for {args.version}:",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "If the build matrix changed, update EXPECTED_DIST_SUFFIXES in "
            "release-plz.yml to match.",
            file=sys.stderr,
        )
        return 1
    print(f"verified {len(names)} distributions against {SUFFIXES_ENV}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="absent | partial | complete on PyPI")
    status.add_argument("version")
    status.set_defaults(func=cmd_status)

    dist = sub.add_parser("dist", help="check a dist/ directory against the set")
    dist.add_argument("dir", type=Path)
    dist.add_argument("version")
    dist.set_defaults(func=cmd_dist)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
