#!/usr/bin/env python3
"""Block manual edits to release-plz-managed Cargo version fields."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from typing import Any


DEPENDENCY_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")
MISSING = object()


@dataclass(frozen=True)
class VersionEdit:
    path: str
    key: str
    before: object
    after: object


def run_git(args: list[str], *, allow_missing: bool = False) -> str | None:
    result = subprocess.run(
        ["git", *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if allow_missing and result.returncode != 0:
        return None
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return result.stdout


def changed_cargo_manifests(base: str, head: str) -> list[str]:
    output = run_git(["diff", "--name-only", "--diff-filter=ACMRT", base, head])
    assert output is not None
    return sorted(
        path
        for path in output.splitlines()
        if path == "Cargo.toml" or path.endswith("/Cargo.toml")
    )


def manifest_at(revision: str, path: str) -> dict[str, Any] | None:
    content = run_git(["show", f"{revision}:{path}"], allow_missing=True)
    if content is None:
        return None
    try:
        return tomllib.loads(content)
    except tomllib.TOMLDecodeError as exc:
        raise SystemExit(f"{path} at {revision} is not valid TOML: {exc}") from exc


def get_path(data: dict[str, Any] | None, keys: tuple[str, ...]) -> object:
    if data is None:
        return MISSING

    current: object = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return MISSING
        current = current[key]
    return current


def explicit_version(value: object) -> object:
    return value if isinstance(value, str) else MISSING


def add_if_changed(
    edits: list[VersionEdit],
    *,
    path: str,
    key: str,
    before: object,
    after: object,
) -> None:
    before_version = explicit_version(before)
    after_version = explicit_version(after)
    if before_version != after_version:
        edits.append(VersionEdit(path, key, before_version, after_version))


def dependency_tables(
    data: dict[str, Any] | None, prefix: tuple[str, ...] = ()
) -> dict[tuple[str, ...], dict[str, Any]]:
    if data is None:
        return {}

    tables: dict[tuple[str, ...], dict[str, Any]] = {}
    for table_name in DEPENDENCY_TABLES:
        table = get_path(data, (*prefix, table_name))
        if isinstance(table, dict):
            tables[(*prefix, table_name)] = table

    workspace = get_path(data, (*prefix, "workspace"))
    if isinstance(workspace, dict):
        for table_name in DEPENDENCY_TABLES:
            table = workspace.get(table_name)
            if isinstance(table, dict):
                tables[(*prefix, "workspace", table_name)] = table

    target = get_path(data, (*prefix, "target"))
    if isinstance(target, dict):
        for target_name, target_data in target.items():
            if isinstance(target_data, dict):
                tables.update(dependency_tables(data, (*prefix, "target", target_name)))

    return tables


def dependency_is_local_path(before: object, after: object) -> bool:
    return (
        (isinstance(before, dict) and "path" in before)
        or (isinstance(after, dict) and "path" in after)
    )


def find_release_version_edits(path: str, base: str, head: str) -> list[VersionEdit]:
    before = manifest_at(base, path)
    after = manifest_at(head, path)
    edits: list[VersionEdit] = []

    add_if_changed(
        edits,
        path=path,
        key="workspace.package.version",
        before=get_path(before, ("workspace", "package", "version")),
        after=get_path(after, ("workspace", "package", "version")),
    )
    add_if_changed(
        edits,
        path=path,
        key="package.version",
        before=get_path(before, ("package", "version")),
        after=get_path(after, ("package", "version")),
    )

    before_tables = dependency_tables(before)
    after_tables = dependency_tables(after)
    for table_path in sorted(before_tables.keys() | after_tables.keys()):
        before_table = before_tables.get(table_path, {})
        after_table = after_tables.get(table_path, {})
        dep_names = sorted(before_table.keys() | after_table.keys())
        for dep_name in dep_names:
            before_dep = before_table.get(dep_name, MISSING)
            after_dep = after_table.get(dep_name, MISSING)
            if not dependency_is_local_path(before_dep, after_dep):
                continue

            before_version = (
                before_dep.get("version", MISSING)
                if isinstance(before_dep, dict)
                else MISSING
            )
            after_version = (
                after_dep.get("version", MISSING)
                if isinstance(after_dep, dict)
                else MISSING
            )
            add_if_changed(
                edits,
                path=path,
                key=".".join((*table_path, dep_name, "version")),
                before=before_version,
                after=after_version,
            )

    return edits


def format_value(value: object) -> str:
    if value is MISSING:
        return "<missing or inherited>"
    return repr(value)


def emit_failure(edits: list[VersionEdit]) -> None:
    print(
        "::error::Versions are managed by release-plz; do not edit `version` "
        "manually. Use a conventional `feat!:`/`BREAKING CHANGE:` commit and "
        "release-plz will compute the version. For deliberate release recovery, "
        "use a `manual-version` PR label.",
        file=sys.stderr,
    )
    print("Blocked release-owned Cargo version edits:", file=sys.stderr)
    for edit in edits:
        print(
            f"- {edit.path}: {edit.key} changed "
            f"{format_value(edit.before)} -> {format_value(edit.after)}",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="HEAD^1")
    parser.add_argument("--head", default="HEAD")
    args = parser.parse_args()

    edits: list[VersionEdit] = []
    for manifest_path in changed_cargo_manifests(args.base, args.head):
        edits.extend(find_release_version_edits(manifest_path, args.base, args.head))

    if edits:
        emit_failure(edits)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
