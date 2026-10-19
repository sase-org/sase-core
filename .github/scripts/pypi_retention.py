#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["packaging"]
# ///
"""Plan and verify PyPI storage retention for ``sase-core-rs``.

Deletion stays human-gated. ``plan`` derives a delete list from live metadata,
``check`` validates any list against the same consumer-driven protected set, and
``pypi_delete.py`` imports this module to enforce that set before login.

Subcommands: ``plan``, ``check``, ``regex``, ``compare``, ``verify``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

import pypi_release_files


PACKAGE = "sase-core-rs"
CONSUMERS = ("sase", "sase-github", "sase-telegram", "sase-research-artifacts")
SUPPORT_HORIZON = 10

# PyPI rejected an upload once the project crossed ~10.73e9 bytes (summing the
# `size` of every file in the JSON API), i.e. 10 GiB, not 10e9 bytes.
PROJECT_LIMIT_BYTES = 10 * 1024**3

DEFAULT_KEEP = 30

# Observed release cadence before the daily-cut change, and the target after it.
CADENCES_PER_DAY = (4.32, 1.0)

# Runway threshold uses the post-cut cadence: one release a day.
DEFAULT_RUNWAY_CADENCE = 1.0
DEFAULT_MIN_RUNWAY_DAYS = 30

# Number of newest releases averaged to estimate the size of the next release.
RECENT_RELEASES = 10

# Header pypi-cleanup logs before listing the versions it selected, one per line.
CLEANUP_HEADER = "Found the following releases of package"
CLEANUP_VERSION_LINE = re.compile(r"^INFO:root: (\S+)\s*$")

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
RELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release-plz.yml"


class RetentionError(Exception):
    """A missing, malformed or incomplete input that makes deletion unsafe."""


@dataclass(frozen=True)
class Release:
    version: Version
    name: str
    files: int
    size: int
    file_records: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class RequirementUse:
    consumer: str
    release: str
    raw: str
    specifier: SpecifierSet

    @property
    def reason(self) -> str:
        return f"{self.consumer} {self.release} requires {self.raw}"


@dataclass(frozen=True)
class ConsumerRelease:
    package: str
    release: Release
    requirements: tuple[RequirementUse, ...]


@dataclass(frozen=True)
class Headroom:
    """Projected PyPI storage runway at a given release cadence."""

    used: int
    free: int
    release_count: int
    file_count: int
    recent_count: int
    average_bytes: int
    remaining_releases: int
    runway_days: float
    cadence: float

    @property
    def computable(self) -> bool:
        return self.average_bytes > 0


@dataclass
class ProtectedSet:
    """Protected releases and the source that makes each one necessary."""

    core_releases: tuple[Release, ...]
    published_versions: tuple[str, ...]
    files_by_version: dict[str, tuple[str, ...]]
    complete_versions: frozenset[str]
    reasons_by_version: dict[str, set[str]] = field(default_factory=dict)
    supported_without_complete: list[str] = field(default_factory=list)
    already_unresolvable: list[str] = field(default_factory=list)

    @property
    def protected_versions(self) -> set[str]:
        return set(self.reasons_by_version)

    @property
    def file_to_version(self) -> dict[str, str]:
        return {
            filename: version
            for version, filenames in self.files_by_version.items()
            for filename in filenames
        }

    def add(self, version: str, reason: str) -> None:
        self.reasons_by_version.setdefault(version, set()).add(reason)

    def violations(self, entries: list[str]) -> list[str]:
        violations: list[str] = []
        file_to_version = self.file_to_version
        version_by_value = {Version(version): version for version in self.published_versions}
        for entry in entries:
            if entry in file_to_version:
                version = file_to_version[entry]
            else:
                try:
                    parsed = Version(entry)
                except InvalidVersion as exc:
                    raise RetentionError(
                        f"delete-list entry is neither a published version nor a published distribution filename: {entry}"
                    ) from exc
                version = version_by_value.get(parsed)
                if version is None and entry.startswith("sase_core_rs-"):
                    raise RetentionError(f"delete-list filename is not published: {entry}")
            if version is None or version not in self.reasons_by_version:
                continue
            reasons = "; ".join(sorted(self.reasons_by_version[version]))
            violations.append(f"{entry} would break {reasons}")
        return violations


class DataSource(Protocol):
    def fetch_project(self, package: str) -> dict[str, Any]: ...

    def fetch_release(self, package: str, version: str) -> dict[str, Any]: ...


def fetch_json(url: str, *, timeout: int, context: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "sase-pypi-retention"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.getcode()
            if status != 200:
                raise RetentionError(f"{context}: PyPI answered HTTP {status} for {url}")
            payload = json.load(response)
    except RetentionError:
        raise
    except (urllib.error.URLError, TimeoutError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        status = f"HTTP {exc.code}" if isinstance(exc, urllib.error.HTTPError) else str(exc)
        raise RetentionError(f"{context}: failed to query {url}: {status}") from exc
    if not isinstance(payload, dict):
        raise RetentionError(f"{context}: malformed JSON from {url}: expected an object")
    return payload


class LiveDataSource:
    def fetch_project(self, package: str) -> dict[str, Any]:
        return fetch_json(
            f"https://pypi.org/pypi/{package}/json", timeout=60, context=f"{package} project metadata"
        )

    def fetch_release(self, package: str, version: str) -> dict[str, Any]:
        return fetch_json(
            f"https://pypi.org/pypi/{package}/{version}/json",
            timeout=30,
            context=f"{package} {version} release metadata",
        )


class FixtureDataSource:
    """Offline source for --fixture-bundle, with the same shapes as the PyPI API."""

    def __init__(self, bundle: dict[str, Any]) -> None:
        self.bundle = bundle

    def fetch_project(self, package: str) -> dict[str, Any]:
        try:
            if package == PACKAGE:
                value = self.bundle["core"]["project"]
            else:
                value = self.bundle["consumers"][package]["project"]
        except (KeyError, TypeError) as exc:
            raise RetentionError(f"fixture bundle is missing {package} project JSON") from exc
        if not isinstance(value, dict):
            raise RetentionError(f"fixture bundle has malformed {package} project JSON")
        return value

    def fetch_release(self, package: str, version: str) -> dict[str, Any]:
        try:
            if package == PACKAGE:
                value = self.bundle["core"]["versions"][version]
            else:
                value = self.bundle["consumers"][package]["versions"][version]
        except (KeyError, TypeError) as exc:
            raise RetentionError(f"fixture bundle is missing {package} {version} release JSON") from exc
        if not isinstance(value, dict):
            raise RetentionError(f"fixture bundle has malformed {package} {version} release JSON")
        return value


class HybridDataSource(LiveDataSource):
    """Legacy --input-json support: use a saved core project response, fetch the rest live."""

    def __init__(self, core_project: dict[str, Any]) -> None:
        self.core_project = core_project

    def fetch_project(self, package: str) -> dict[str, Any]:
        return self.core_project if package == PACKAGE else super().fetch_project(package)


def load_fixture_bundle(path: Path) -> FixtureDataSource:
    try:
        bundle = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise RetentionError(f"cannot read fixture bundle {path}: {exc}") from exc
    if not isinstance(bundle, dict) or not isinstance(bundle.get("core"), dict) or not isinstance(
        bundle.get("consumers"), dict
    ):
        raise RetentionError(f"fixture bundle {path} must contain core and consumers objects")
    return FixtureDataSource(bundle)


def source_for_args(args: argparse.Namespace) -> DataSource:
    fixture_bundle = getattr(args, "fixture_bundle", None)
    input_json = getattr(args, "input_json", None)
    if fixture_bundle and input_json:
        raise RetentionError("use either --fixture-bundle or --input-json, not both")
    if fixture_bundle:
        return load_fixture_bundle(Path(fixture_bundle))
    if input_json:
        path = Path(input_json)
        try:
            project = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RetentionError(f"cannot read core project JSON {path}: {exc}") from exc
        if not isinstance(project, dict):
            raise RetentionError(f"core project JSON {path} must be an object")
        return HybridDataSource(project)
    return LiveDataSource()


def load_releases(project: dict[str, Any], *, context: str = "PyPI project JSON") -> list[Release]:
    """Published releases, oldest first. Empty releases do not exist on PyPI."""
    release_map = project.get("releases")
    if not isinstance(release_map, dict):
        raise RetentionError(f"{context}: malformed JSON; missing a releases object")
    releases: list[Release] = []
    seen_versions: set[Version] = set()
    for name, files in release_map.items():
        if not isinstance(name, str) or not isinstance(files, list):
            raise RetentionError(f"{context}: malformed release entry {name!r}")
        if not files:
            continue
        try:
            version = Version(name)
        except InvalidVersion as exc:
            raise RetentionError(f"{context}: invalid published version {name!r}") from exc
        if version in seen_versions:
            raise RetentionError(f"{context}: duplicate semantic version {name!r}")
        seen_versions.add(version)
        for file in files:
            if not isinstance(file, dict) or not isinstance(file.get("filename"), str):
                raise RetentionError(f"{context}: malformed file entry for release {name}")
        releases.append(
            Release(
                version=version,
                name=name,
                files=len(files),
                size=_file_size_total(files, context, name),
                file_records=tuple(files),
            )
        )
    return sorted(releases, key=lambda release: release.version)


def _file_size_total(files: list[dict[str, Any]], context: str, version: str) -> int:
    try:
        return sum(int(file.get("size", 0)) for file in files)
    except (TypeError, ValueError) as exc:
        raise RetentionError(f"{context}: malformed file size for release {version}") from exc


def read_expected_suffixes(path: Path = RELEASE_WORKFLOW) -> list[str]:
    """Read the sole expected-file set from release-plz.yml and use its shared parser."""
    try:
        text = path.read_text()
    except OSError as exc:
        raise RetentionError(f"cannot read expected distribution set from {path}: {exc}") from exc
    match = re.search(r"(?m)^  EXPECTED_DIST_SUFFIXES: \|\n((?:    \S[^\n]*(?:\n|$))+)", text)
    if match is None:
        raise RetentionError(f"cannot find EXPECTED_DIST_SUFFIXES in {path}")
    try:
        return pypi_release_files.parse_suffixes(match.group(1))
    except SystemExit as exc:
        raise RetentionError(f"invalid EXPECTED_DIST_SUFFIXES in {path}: {exc}") from exc


def is_yanked(file: dict[str, Any]) -> bool:
    return bool(file.get("yanked"))


def release_urls(payload: dict[str, Any], package: str, version: str) -> list[dict[str, Any]]:
    urls = payload.get("urls")
    if not isinstance(urls, list):
        raise RetentionError(f"{package} {version}: malformed release JSON; missing a urls list")
    for file in urls:
        if not isinstance(file, dict) or not isinstance(file.get("filename"), str):
            raise RetentionError(f"{package} {version}: malformed distribution file metadata")
    return urls


def consumer_requirements(payload: dict[str, Any], package: str, version: str) -> tuple[RequirementUse, ...]:
    info = payload.get("info")
    if not isinstance(info, dict) or "requires_dist" not in info:
        raise RetentionError(f"{package} {version}: malformed release JSON; missing info.requires_dist")
    raw_requirements = info["requires_dist"]
    if raw_requirements is None:
        raw_requirements = []
    if not isinstance(raw_requirements, list) or any(not isinstance(raw, str) for raw in raw_requirements):
        raise RetentionError(f"{package} {version}: malformed info.requires_dist list")
    uses: list[RequirementUse] = []
    for raw in raw_requirements:
        try:
            requirement = Requirement(raw)
        except InvalidRequirement as exc:
            # We cannot prove an invalid metadata string is unrelated to the core.
            raise RetentionError(f"{package} {version}: cannot parse requirement {raw!r}: {exc}") from exc
        if canonicalize_name(requirement.name) != canonicalize_name(PACKAGE):
            continue
        # Markers and extras are deliberately ignored: each declared core window is
        # protected conservatively, regardless of the consumer's current environment.
        uses.append(RequirementUse(package, version, raw, requirement.specifier))
    return tuple(uses)


def has_upper_bound(specifier: SpecifierSet) -> bool:
    return any(part.operator in {"<", "<=", "==", "===", "~="} for part in specifier)


def matching_versions(
    use: RequirementUse, core_releases: list[Release], complete_versions: set[str]
) -> tuple[list[str], list[str]]:
    published = [
        release.name for release in core_releases if use.specifier.contains(release.version, prereleases=True)
    ]
    complete = [version for version in published if version in complete_versions]
    return published, complete


def add_requirement_window(
    protected: ProtectedSet,
    use: RequirementUse,
    core_releases: list[Release],
    *,
    reason_prefix: str = "",
) -> tuple[list[str], list[str]]:
    published, complete = matching_versions(use, core_releases, set(protected.complete_versions))
    if has_upper_bound(use.specifier):
        selected = published
    else:
        selected = complete[-1:]
    reason = f"{reason_prefix}{use.reason}"
    for version in selected:
        protected.add(version, reason)
    return published, complete


def compute_protected_set(
    source: DataSource | None = None,
    *,
    keep_count: int = DEFAULT_KEEP,
    keep_versions: list[str] | None = None,
    support_horizon: int = SUPPORT_HORIZON,
    expected_suffixes: list[str] | None = None,
) -> ProtectedSet:
    """Compute the complete protected set, failing on every incomplete evidence path."""
    if support_horizon < SUPPORT_HORIZON:
        raise RetentionError(f"support horizon must be at least {SUPPORT_HORIZON}")
    if keep_count < DEFAULT_KEEP:
        raise RetentionError(f"keep count must be at least {DEFAULT_KEEP}")
    source = source or LiveDataSource()
    suffixes = expected_suffixes if expected_suffixes is not None else read_expected_suffixes()

    core_project = source.fetch_project(PACKAGE)
    core_releases = load_releases(core_project, context=f"{PACKAGE} project JSON")
    if not core_releases:
        raise RetentionError(f"{PACKAGE} project JSON contains no published releases")

    file_names: dict[str, tuple[str, ...]] = {}
    complete_versions: set[str] = set()
    for release in core_releases:
        payload = source.fetch_release(PACKAGE, release.name)
        urls = release_urls(payload, PACKAGE, release.name)
        if not urls:
            raise RetentionError(f"{PACKAGE} {release.name}: project JSON lists files but release JSON is empty")
        file_names[release.name] = tuple(file["filename"] for file in urls)
        try:
            result = pypi_release_files.classify(release.name, urls, suffixes)
        except (KeyError, TypeError) as exc:
            raise RetentionError(f"{PACKAGE} {release.name}: malformed distribution file metadata: {exc}") from exc
        if result.state == pypi_release_files.COMPLETE:
            complete_versions.add(release.name)

    protected = ProtectedSet(
        core_releases=tuple(core_releases),
        published_versions=tuple(release.name for release in core_releases),
        files_by_version=file_names,
        complete_versions=frozenset(complete_versions),
    )

    consumer_releases: dict[str, list[ConsumerRelease]] = {}
    supported_releases: dict[str, list[ConsumerRelease]] = {}
    for package in CONSUMERS:
        project = source.fetch_project(package)
        releases = load_releases(project, context=f"{package} project JSON")
        available = [
            release
            for release in reversed(releases)
            if any(not is_yanked(file) for file in release.file_records)
        ]
        if not available:
            raise RetentionError(f"{package} project JSON has no non-yanked published releases")
        supported = available[:support_horizon]
        # The baseline and ALREADY UNRESOLVABLE report inspect sase's full history.
        # Other family consumers need only their supported horizon.
        inspect = available if package == "sase" else supported
        rows: list[ConsumerRelease] = []
        for release in inspect:
            payload = source.fetch_release(package, release.name)
            uses = consumer_requirements(payload, package, release.name)
            rows.append(ConsumerRelease(package, release, uses))
        consumer_releases[package] = rows
        supported_releases[package] = rows[: len(supported)]

    # Supported releases contribute every bounded version, or the newest complete
    # version for an unbounded requirement. A warning never blocks reclaim by itself.
    for package, rows in supported_releases.items():
        for row in rows:
            for use in row.requirements:
                published, complete = add_requirement_window(protected, use, core_releases)
                if not complete:
                    protected.supported_without_complete.append(
                        f"{use.consumer} {use.release} has no complete {PACKAGE} version for {use.raw}"
                    )
                if not published:
                    # Historical broken windows are reported separately below; current
                    # supported releases still get their direct warning above.
                    continue

    # Confirmed-working baseline: the two newest sase releases, of any age, whose
    # dependency window currently contains at least one complete core version.
    # E2 adds candidate-manifest-qualified versions at this union boundary; it does
    # not change the consumer or retention rules implemented here.
    baseline_count = 0
    for row in consumer_releases["sase"]:
        if baseline_count >= 2:
            break
        if not any(
            matching_versions(use, core_releases, set(protected.complete_versions))[1]
            for use in row.requirements
        ):
            continue
        baseline_count += 1
        for use in row.requirements:
            add_requirement_window(
                protected,
                use,
                core_releases,
                reason_prefix="confirmed-working baseline: ",
            )

    # Releases whose windows have no published core version are already broken. They
    # are evidence for recovery/backfill, not a reason to block an otherwise safe plan.
    for row in consumer_releases["sase"]:
        if row.requirements and not any(
            matching_versions(use, core_releases, set(protected.complete_versions))[0]
            for use in row.requirements
        ):
            protected.already_unresolvable.append(
                f"sase {row.release.name}: " + "; ".join(use.raw for use in row.requirements)
            )

    # The unchanged retention floor and explicit human-selected versions.
    newest = core_releases[-keep_count:]
    for release in newest:
        protected.add(release.name, f"newest {keep_count} {PACKAGE} releases")
    published = set(protected.published_versions)
    pinned = set(keep_versions or [])
    unknown = pinned - published
    if unknown:
        raise RetentionError(f"--keep-version names unpublished versions: {sorted(unknown)}")
    for version in pinned:
        protected.add(version, "explicit --keep-version")
    return protected


def read_list(path: Path) -> list[str]:
    try:
        versions = [
            line.strip()
            for line in path.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
    except OSError as exc:
        raise RetentionError(f"cannot read delete list {path}: {exc}") from exc
    if len(versions) != len(set(versions)):
        raise RetentionError(f"{path} lists an entry more than once")
    return versions


def version_regex(versions: list[str]) -> str:
    """Anchored regex; pypi-cleanup uses ``re.match``, which only anchors the start."""
    if not versions:
        raise RetentionError("refusing to build a regex from an empty delete list")
    return "^(?:" + "|".join(re.escape(v) for v in versions) + ")$"


def abbreviate(versions: list[str], limit: int = 8) -> str:
    if len(versions) <= limit:
        return str(versions)
    return f"{versions[:limit]} ... and {len(versions) - limit} more"


def gb(size: int) -> str:
    return f"{size / 1e9:.2f} GB"


def compute_headroom(
    releases: list[Release],
    *,
    limit: int = PROJECT_LIMIT_BYTES,
    cadence: float = DEFAULT_RUNWAY_CADENCE,
) -> Headroom:
    """Project remaining releases and days of storage from current occupancy."""
    used = sum(release.size for release in releases)
    free = limit - used
    recent = releases[-RECENT_RELEASES:]
    average = sum(release.size for release in recent) // max(len(recent), 1) if recent else 0
    if average <= 0 or free <= 0:
        remaining = 0
        runway = 0.0
    else:
        remaining = free // average
        runway = remaining / cadence
    return Headroom(
        used=used,
        free=free,
        release_count=len(releases),
        file_count=sum(release.files for release in releases),
        recent_count=len(recent),
        average_bytes=average,
        remaining_releases=remaining,
        runway_days=runway,
        cadence=cadence,
    )


def report_headroom(releases: list[Release]) -> Headroom:
    headroom = compute_headroom(releases)
    print(
        f"  used {gb(headroom.used)} ({headroom.used / PROJECT_LIMIT_BYTES:.1%} of "
        f"{PROJECT_LIMIT_BYTES / 1024**3:.0f} GiB), free {gb(headroom.free)}"
    )
    if headroom.average_bytes <= 0 or headroom.free <= 0:
        return headroom
    print(
        f"  average newest-{headroom.recent_count} release "
        f"{headroom.average_bytes / 1e6:.1f} MB -> ~{headroom.remaining_releases} more releases"
    )
    for cadence in CADENCES_PER_DAY:
        print(f"    runway at {cadence:g} releases/day: ~{headroom.remaining_releases / cadence:.0f} days")
    return headroom


def print_protection(protected: ProtectedSet) -> None:
    if protected.already_unresolvable:
        print("ALREADY UNRESOLVABLE (see docs/pypi-retention.md, Core backfill):")
        for item in protected.already_unresolvable:
            print(f"  {item}")
    for warning in protected.supported_without_complete:
        print(f"WARNING: {warning}")
    print("protected core releases and all of their published files:")
    version_order = {version: Version(version) for version in protected.published_versions}
    for version in sorted(protected.reasons_by_version, key=version_order.__getitem__):
        files = protected.files_by_version.get(version, ())
        print(f"  {version} ({len(files)} published files)")
        for reason in sorted(protected.reasons_by_version[version]):
            print(f"    by {reason}")
        for filename in files:
            print(f"    {filename}")


def cmd_plan(args: argparse.Namespace) -> int:
    if args.package != PACKAGE:
        raise RetentionError(f"consumer-driven retention only supports {PACKAGE}")
    source = source_for_args(args)
    protected = compute_protected_set(
        source,
        keep_count=args.keep,
        keep_versions=args.keep_version,
        support_horizon=args.support_horizon,
    )
    core_releases = list(protected.core_releases)
    protected_names = protected.protected_versions
    keep = [release for release in core_releases if release.name in protected_names]
    delete = [release for release in core_releases if release.name not in protected_names]

    # Reuse exactly the public validator before writing any generated list.
    violations = protected.violations([release.name for release in delete])
    if violations:
        raise RetentionError("internal retention plan selected a protected release: " + "; ".join(violations))

    print_protection(protected)
    if not delete:
        print("nothing to delete: every published version is inside the protected set")
        return 0

    kept_size = sum(release.size for release in keep)
    freed = sum(release.size for release in delete)
    total = kept_size + freed
    print(
        f"{args.package}: {len(core_releases)} releases, "
        f"{sum(release.files for release in core_releases)} files, {gb(total)} now"
    )
    print(f"keep   {len(keep):>3}: {gb(kept_size)}")
    print(f"delete {len(delete):>3} ({delete[0].name} .. {delete[-1].name}): {gb(freed)}")
    print("after deletion:")
    report_headroom(keep)

    output = Path(args.output)
    try:
        output.write_text("".join(f"{release.name}\n" for release in delete))
    except OSError as exc:
        raise RetentionError(f"cannot write delete list {output}: {exc}") from exc
    print(f"wrote {len(delete)} versions to {output}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    if args.package != PACKAGE:
        raise RetentionError(f"consumer-driven retention only supports {PACKAGE}")
    source = source_for_args(args)
    protected = compute_protected_set(
        source,
        keep_count=args.keep,
        keep_versions=args.keep_version,
        support_horizon=args.support_horizon,
    )
    entries = read_list(Path(args.list))
    print_protection(protected)
    violations = protected.violations(entries)
    if violations:
        print("REFUSED: delete list would break protected releases:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1
    print(f"OK: {len(entries)} delete-list entries do not touch the protected set")
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
        raise RetentionError(
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
        try:
            project = json.loads(Path(args.input_json).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise RetentionError(f"cannot read core project JSON {args.input_json}: {exc}") from exc
        if not isinstance(project, dict):
            raise RetentionError(f"core project JSON {args.input_json} must be an object")
    else:
        project = LiveDataSource().fetch_project(args.package)
    releases = load_releases(project, context=f"{args.package} project JSON")
    published = {release.name for release in releases}
    used = sum(release.size for release in releases)
    files = sum(release.files for release in releases)
    print(f"{args.package}: {len(releases)} releases, {files} files, {gb(used)}")
    headroom = report_headroom(releases)
    expected_files = len(read_expected_suffixes())
    partial = [release for release in releases if release.files < expected_files]
    if partial:
        print(
            f"  partial releases (fewer than {expected_files} files): "
            + abbreviate([f"{release.name} ({release.files})" for release in partial])
        )

    problems: list[str] = []
    if not headroom.computable:
        problems.append("cannot compute runway: newest-release average size is 0")
    elif headroom.runway_days < args.min_runway_days:
        problems.append(
            f"projected runway is {headroom.runway_days:.0f} days at "
            f"{headroom.cadence:g} release/day; minimum is {args.min_runway_days}"
        )
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
    if args.expect_oldest is not None and (not releases or releases[0].name != args.expect_oldest):
        found = releases[0].name if releases else "none"
        problems.append(f"expected {args.expect_oldest} to be the oldest release, found {found}")
    if used >= PROJECT_LIMIT_BYTES:
        problems.append(f"project is at or over the {PROJECT_LIMIT_BYTES} byte limit")
    sys.stdout.flush()
    for problem in problems:
        print(f"FAIL: {problem}", file=sys.stderr)
    if not problems:
        print("OK")
    return 1 if problems else 0


def support_horizon_arg(value: str) -> int:
    try:
        horizon = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("support horizon must be an integer") from exc
    if horizon < SUPPORT_HORIZON:
        raise argparse.ArgumentTypeError(f"support horizon cannot be lower than {SUPPORT_HORIZON}")
    return horizon


def keep_count_arg(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("keep count must be an integer") from exc
    if count < DEFAULT_KEEP:
        raise argparse.ArgumentTypeError(f"keep count cannot be lower than {DEFAULT_KEEP}")
    return count


def min_runway_days_arg(value: str) -> int:
    try:
        days = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("min runway days must be an integer") from exc
    if days < 0:
        raise argparse.ArgumentTypeError("min runway days cannot be negative")
    return days


def add_protection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--keep",
        type=keep_count_arg,
        default=DEFAULT_KEEP,
        help=f"keep at least this many newest core releases (default: {DEFAULT_KEEP})",
    )
    parser.add_argument(
        "--keep-version",
        action="append",
        default=[],
        help="also keep this core version (repeatable)",
    )
    parser.add_argument(
        "--support-horizon",
        type=support_horizon_arg,
        default=SUPPORT_HORIZON,
        help=f"newest non-yanked releases per consumer to support (minimum: {SUPPORT_HORIZON})",
    )
    parser.add_argument(
        "--fixture-bundle",
        help="offline JSON with core/consumer project and per-version PyPI API responses",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--package", default=PACKAGE)
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="derive the delete list from live consumer and PyPI metadata")
    add_protection_arguments(plan)
    plan.add_argument("--output", required=True, help="file to write the delete list to")
    plan.add_argument(
        "--input-json",
        help="legacy core project JSON override; consumer and per-version metadata is still fetched live",
    )
    plan.set_defaults(handler=cmd_plan)

    check = sub.add_parser("check", help="refuse delete-list entries that would break a supported release")
    add_protection_arguments(check)
    check.add_argument("--list", required=True, help="delete list of versions or distribution filenames")
    check.set_defaults(handler=cmd_check)

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
    verify.add_argument("--input-json", help="read this saved PyPI project JSON instead of querying PyPI")
    verify.add_argument(
        "--min-runway-days",
        type=min_runway_days_arg,
        default=DEFAULT_MIN_RUNWAY_DAYS,
        help=(
            "fail when projected days of storage at one release a day fall below this "
            f"(default: {DEFAULT_MIN_RUNWAY_DAYS})"
        ),
    )
    verify.set_defaults(handler=cmd_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        return args.handler(args)
    except RetentionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
