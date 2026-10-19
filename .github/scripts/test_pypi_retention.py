"""Offline tests for consumer-driven PyPI retention and delete-list validation."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pypi_release_files  # noqa: E402
import pypi_retention as retention  # noqa: E402


SUFFIXES = retention.read_expected_suffixes()


def core_files(version: str, *, yanked: tuple[str, ...] = ()) -> list[dict[str, object]]:
    result = []
    for suffix in SUFFIXES:
        filename = (
            f"sase_core_rs-{version}.tar.gz"
            if suffix == ".tar.gz"
            else f"sase_core_rs-{version}-cp312-abi3-{suffix}"
        )
        result.append({"filename": filename, "yanked": suffix in yanked, "size": 100})
    return result


def package_files(package: str, version: str) -> list[dict[str, object]]:
    return [{"filename": f"{package.replace('-', '_')}-{version}-py3-none-any.whl", "yanked": False}]


def make_bundle(
    *,
    core_versions: list[str] | None = None,
    sase_requirements: dict[str, list[str]] | None = None,
    github_requirements: list[str] | None = None,
    sase_versions: list[str] | None = None,
) -> dict[str, object]:
    core_versions = core_versions or ["0.31.0", "0.42.0", "0.42.1", "0.43.0", "0.44.0"]
    sase_versions = sase_versions or ["0.17.1", "0.18.0"]
    sase_requirements = sase_requirements or {
        "0.17.1": ["sase-core-rs>=0.32.16,<0.33.0"],
        "0.18.0": ["sase-core-rs>=0.42.0,<0.43.0"],
    }
    consumer_versions = {
        "sase": sase_versions,
        "sase-github": ["0.2.24"],
        "sase-telegram": ["0.4.29"],
        "sase-research-artifacts": ["0.2.0"],
    }
    consumer_requirements: dict[str, dict[str, list[str]]] = {
        "sase": sase_requirements,
        "sase-github": {"0.2.24": github_requirements or []},
        "sase-telegram": {"0.4.29": []},
        "sase-research-artifacts": {"0.2.0": []},
    }
    core_project_releases = {
        version: core_files(version) for version in core_versions
    }
    core_release_json = {
        version: {"urls": core_files(version, yanked=("win_amd64.whl",) if version == "0.43.0" else ())}
        for version in core_versions
    }
    consumers = {}
    for package, versions in consumer_versions.items():
        consumers[package] = {
            "project": {"releases": {version: package_files(package, version) for version in versions}},
            "versions": {
                version: {"info": {"requires_dist": consumer_requirements[package].get(version, [])}}
                for version in versions
            },
        }
    return {
        "core": {
            "project": {"releases": core_project_releases},
            "versions": core_release_json,
        },
        "consumers": consumers,
    }


class ProtectedSetTests(unittest.TestCase):
    def compute(self, bundle: dict[str, object] | None = None) -> retention.ProtectedSet:
        return retention.compute_protected_set(retention.FixtureDataSource(bundle or make_bundle()))

    def test_bounded_window_protects_every_matching_version_and_file(self) -> None:
        protected = self.compute()
        reason = "sase 0.18.0 requires sase-core-rs>=0.42.0,<0.43.0"
        for version in ("0.42.0", "0.42.1"):
            self.assertIn(reason, protected.reasons_by_version[version])
            self.assertEqual(len(protected.files_by_version[version]), len(SUFFIXES))
            self.assertEqual(len(protected.files_by_version[version]), 5)

    def test_yanked_core_release_is_not_complete(self) -> None:
        protected = self.compute()
        self.assertIn("0.42.0", protected.complete_versions)
        self.assertNotIn("0.43.0", protected.complete_versions)

    def test_exact_requirement_is_a_single_version_window(self) -> None:
        bundle = make_bundle(
            sase_requirements={"0.17.1": [], "0.18.0": ["sase-core-rs==0.42.0"]}
        )
        protected = self.compute(bundle)
        self.assertIn("sase 0.18.0 requires sase-core-rs==0.42.0", protected.reasons_by_version["0.42.0"])
        self.assertNotIn("sase 0.18.0 requires sase-core-rs==0.42.0", protected.reasons_by_version["0.42.1"])

    def test_unbounded_requirement_protects_newest_complete_match(self) -> None:
        bundle = make_bundle(
            sase_requirements={"0.17.1": [], "0.18.0": []},
            github_requirements=["sase-core-rs>=0.42.0"],
        )
        protected = self.compute(bundle)
        reason = "sase-github 0.2.24 requires sase-core-rs>=0.42.0"
        self.assertIn(reason, protected.reasons_by_version["0.44.0"])
        self.assertNotIn(reason, protected.reasons_by_version.get("0.42.0", set()))

    def test_baseline_reaches_older_working_sase_releases(self) -> None:
        versions = [f"0.{minor}.0" for minor in range(10, 22)]
        requirements = {version: [] for version in versions}
        requirements["0.10.0"] = ["sase-core-rs>=0.42.0,<0.43.0"]
        requirements["0.11.0"] = ["sase-core-rs>=0.42.0,<0.43.0"]
        bundle = make_bundle(sase_versions=versions, sase_requirements=requirements)
        protected = self.compute(bundle)
        self.assertIn(
            "confirmed-working baseline: sase 0.11.0 requires sase-core-rs>=0.42.0,<0.43.0",
            protected.reasons_by_version["0.42.0"],
        )
        self.assertIn(
            "confirmed-working baseline: sase 0.10.0 requires sase-core-rs>=0.42.0,<0.43.0",
            protected.reasons_by_version["0.42.1"],
        )

    def test_old_empty_window_is_reported_without_becoming_protected(self) -> None:
        core_versions = ["0.31.0", *(f"0.{minor}.0" for minor in range(32, 62))]
        protected = self.compute(make_bundle(core_versions=core_versions))
        self.assertTrue(any("sase 0.17.1" in item for item in protected.already_unresolvable))
        # This version falls outside the newest-30 floor and has no consumer window,
        # so an already-broken release does not prevent a safe reclaim.
        self.assertEqual(protected.violations(["0.31.0"]), [])

    def test_support_horizon_cannot_drop_below_ten(self) -> None:
        with self.assertRaisesRegex(retention.RetentionError, "at least 10"):
            retention.compute_protected_set(
                retention.FixtureDataSource(make_bundle()), support_horizon=9
            )

    def test_list_violation_names_consumer_requirement_and_distribution(self) -> None:
        protected = self.compute()
        filename = next(
            name for name in protected.files_by_version["0.42.0"] if name.endswith("win_amd64.whl")
        )
        version_violation = protected.violations(["0.42.0"])
        self.assertEqual(len(version_violation), 1)
        self.assertIn("0.42.0 would break", version_violation[0])
        self.assertIn("sase 0.18.0 requires sase-core-rs>=0.42.0,<0.43.0", version_violation[0])
        violation = protected.violations([filename])
        self.assertEqual(len(violation), 1)
        self.assertIn(filename, violation[0])
        self.assertIn("sase 0.18.0 requires sase-core-rs>=0.42.0,<0.43.0", violation[0])


class FailClosedTests(unittest.TestCase):
    def test_pypi_url_error_fails_closed(self) -> None:
        with mock.patch.object(retention.urllib.request, "urlopen", side_effect=urllib.error.URLError("offline")):
            with self.assertRaisesRegex(retention.RetentionError, "failed to query"):
                retention.fetch_json("https://pypi.org/pypi/sase/json", timeout=1, context="sase")

    def test_pypi_5xx_fails_closed(self) -> None:
        failure = urllib.error.HTTPError("https://pypi.org/pypi/sase/json", 503, "unavailable", None, None)
        with mock.patch.object(retention.urllib.request, "urlopen", side_effect=failure):
            with self.assertRaisesRegex(retention.RetentionError, "HTTP 503"):
                retention.fetch_json("https://pypi.org/pypi/sase/json", timeout=1, context="sase")

    def test_pypi_404_fails_closed(self) -> None:
        failure = urllib.error.HTTPError("https://pypi.org/pypi/sase/json", 404, "missing", None, None)
        with mock.patch.object(retention.urllib.request, "urlopen", side_effect=failure):
            with self.assertRaisesRegex(retention.RetentionError, "HTTP 404"):
                retention.fetch_json("https://pypi.org/pypi/sase/json", timeout=1, context="sase")

    def test_malformed_json_fails_closed(self) -> None:
        response = io.BytesIO(b"{")
        response.getcode = lambda: 200  # type: ignore[attr-defined,method-assign]
        with mock.patch.object(retention.urllib.request, "urlopen", return_value=response):
            with self.assertRaisesRegex(retention.RetentionError, "failed to query"):
                retention.fetch_json("https://pypi.org/pypi/sase/json", timeout=1, context="sase")

    def test_404_for_listed_consumer_fails_closed(self) -> None:
        class MissingConsumer(retention.FixtureDataSource):
            def fetch_project(self, package: str) -> dict[str, object]:
                if package == "sase-research-artifacts":
                    raise retention.RetentionError("HTTP 404")
                return super().fetch_project(package)

        with self.assertRaisesRegex(retention.RetentionError, "404"):
            retention.compute_protected_set(MissingConsumer(make_bundle()))

    def test_failed_per_version_request_fails_closed(self) -> None:
        bundle = make_bundle()
        del bundle["core"]["versions"]["0.42.0"]  # type: ignore[index]
        with self.assertRaisesRegex(retention.RetentionError, "missing sase-core-rs 0.42.0"):
            retention.compute_protected_set(retention.FixtureDataSource(bundle))

    def test_expected_file_set_read_failure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(retention.RetentionError, "cannot read expected"):
                retention.read_expected_suffixes(Path(tmp) / "missing.yml")

    def test_plan_failure_does_not_write_a_delete_list(self) -> None:
        bundle = make_bundle()
        del bundle["core"]["versions"]["0.42.0"]  # type: ignore[index]
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "delete-list.txt"
            fixture = Path(tmp) / "fixture.json"
            fixture.write_text(json.dumps(bundle))
            args = argparse.Namespace(
                fixture_bundle=str(fixture),
                input_json=None,
                keep=retention.DEFAULT_KEEP,
                keep_version=[],
                support_horizon=retention.SUPPORT_HORIZON,
                output=str(output),
                package=retention.PACKAGE,
            )
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(retention.RetentionError):
                    retention.cmd_plan(args)
            self.assertFalse(output.exists())


class CliTests(unittest.TestCase):
    def test_check_refuses_planted_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "fixture.json"
            delete_list = Path(tmp) / "delete-list.txt"
            fixture.write_text(json.dumps(make_bundle()))
            delete_list.write_text("0.42.0\n")
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = retention.main(["check", "--fixture-bundle", str(fixture), "--list", str(delete_list)])
            self.assertEqual(code, 1)
            self.assertIn("0.42.0 would break", stderr.getvalue())
            self.assertIn("sase 0.18.0 requires", stderr.getvalue())

    def test_check_refuses_planted_single_wheel(self) -> None:
        protected = retention.compute_protected_set(retention.FixtureDataSource(make_bundle()))
        filename = next(
            name for name in protected.files_by_version["0.42.0"] if name.endswith("win_amd64.whl")
        )
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp) / "fixture.json"
            delete_list = Path(tmp) / "delete-list.txt"
            fixture.write_text(json.dumps(make_bundle()))
            delete_list.write_text(f"{filename}\n")
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = retention.main(["check", "--fixture-bundle", str(fixture), "--list", str(delete_list)])
            self.assertEqual(code, 1)
            self.assertIn(filename, stderr.getvalue())
            self.assertIn("sase 0.18.0 requires", stderr.getvalue())


class SharedFileSetTests(unittest.TestCase):
    def test_workflow_suffixes_use_shared_release_file_parser(self) -> None:
        suffixes = retention.read_expected_suffixes()
        self.assertEqual(len(suffixes), 5)
        self.assertEqual(
            pypi_release_files.classify(
                "0.42.0", core_files("0.42.0"), suffixes
            ).state,
            pypi_release_files.COMPLETE,
        )


if __name__ == "__main__":
    unittest.main()
