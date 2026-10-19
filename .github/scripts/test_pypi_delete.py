"""Offline tests for the pre-credential retention guard in pypi_delete.py."""

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pypi_delete as delete  # noqa: E402
import pypi_retention as retention  # noqa: E402
from test_pypi_retention import make_bundle  # noqa: E402


class DeleteProtectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.protected = retention.compute_protected_set(retention.FixtureDataSource(make_bundle()))
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.list_path = Path(self.tmp.name) / "delete-list.txt"
        self.list_path.write_text("0.42.0\n")
        self.args = delete.build_parser().parse_args(
            ["--list", str(self.list_path), "--confirm", "--username", "tester"]
        )

    def test_protected_list_is_refused_before_credentials_are_read(self) -> None:
        credentials = mock.Mock()
        with mock.patch.object(delete.retention, "compute_protected_set", return_value=self.protected):
            with self.assertRaises(delete.Protected) as raised:
                delete.run(self.args, credentials)
        self.assertEqual(raised.exception.kind, "protected")
        self.assertIn("sase 0.18.0 requires", str(raised.exception))
        credentials.get.assert_not_called()

    def test_unavailable_protected_set_is_refused_before_credentials(self) -> None:
        credentials = mock.Mock()
        with mock.patch.object(
            delete.retention,
            "compute_protected_set",
            side_effect=retention.RetentionError("PyPI HTTP 503"),
        ):
            with self.assertRaises(delete.Protected) as raised:
                delete.run(self.args, credentials)
        self.assertIn("cannot compute the live protected set", str(raised.exception))
        credentials.get.assert_not_called()

    def test_protected_error_kind_uses_refusal_exit_code(self) -> None:
        parser = mock.Mock()
        parser.parse_args.return_value = self.args
        stdout = io.StringIO()
        with mock.patch.object(delete, "build_parser", return_value=parser), mock.patch.object(
            delete, "run", side_effect=delete.Protected("protected test")
        ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(io.StringIO()):
            code = delete.main()
        self.assertEqual(code, 2)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "refused")
        self.assertEqual(payload["error_kind"], "protected")


if __name__ == "__main__":
    unittest.main()
