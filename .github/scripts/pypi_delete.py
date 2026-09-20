#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["packaging", "requests"]
# ///
"""Delete reviewed ``sase-core-rs`` releases from PyPI, surviving the login-confirmation wall.

PyPI has no delete API, so deletion is a scripted web login. ``pypi-cleanup`` does the same
thing and dies the moment PyPI answers a login from an unrecognized device with a
"confirm this login from the same device" interstitial: its ``requests.Session`` exits with
it, so the emailed confirmation link can never confirm anything afterwards. This driver
mirrors the small login/delete flow ``pypi-cleanup`` 0.1.10 performs (``pypi_cleanup/__init__.py``)
but keeps one session alive across the confirmation. When the release page has no delete
form it finds the confirmation email through ``gog``, opens the link **with the same
session**, and polls until the form appears.

It never prompts: the password and the TOTP come from environment variables, the delete list
from a file, and there is no TTY. It prints exactly one JSON object on stdout; every
diagnostic goes to stderr. The exit status is 0 only for ``"status": "deleted"``.

The delete list must come from ``pypi_retention.py plan`` (see ``docs/pypi-retention.md``);
this script deletes exactly what it is given, so it refuses an empty list, a missing
``--confirm``, and any list that reaches a version the caller said to protect.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import urlparse

import requests
from packaging.version import InvalidVersion, Version

PACKAGE = "sase-core-rs"
DEFAULT_BASE_URL = "https://pypi.org"

DEFAULT_PASSWORD_ENV = "PYPI_CLEANUP_PASSWORD"
DEFAULT_TOTP_ENV = "PYPI_TOTP"
DEFAULT_USERNAME_ENV = "PYPI_USERNAME"

# `[PyPI] Unrecognized login to your PyPI account`, the mail PyPI sends when it parks a login.
CONFIRM_SUBJECT = "Unrecognized login to your PyPI account"
CONFIRM_QUERY = f'from:noreply@pypi.org subject:"{CONFIRM_SUBJECT}" newer_than:1d'
CONFIRM_URL = re.compile(r"https?://[^\s<>\"']+/account/confirm-login/\?token=[^\s<>\"']+")
# How many of the newest confirmation emails one poll reads in full.
CONFIRM_CANDIDATES = 5

DEFAULT_WALL_TIMEOUT_SECONDS = 600
DEFAULT_POLL_INTERVAL_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 60
DELETE_ATTEMPTS = 3

# The password and TOTP, scrubbed from every log line and every JSON error string. Numbers in the
# result are never scrubbed, so a six-digit TOTP cannot corrupt a byte count.
_SECRETS: list[str] = []


class DriverError(Exception):
    """A run-ending failure with a stable ``kind`` the caller can branch on."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class Refused(DriverError):
    """The request was rejected before any network traffic."""

    def __init__(self, message: str) -> None:
        super().__init__("refused", message)


@dataclass
class Outcome:
    requested: int
    deleted: int = 0
    already_gone: int = 0
    failed_versions: list[str] = field(default_factory=list)
    wall_encountered: bool = False
    confirmation_source: str = "not_needed"
    confirmation_email_at: str | None = None


@dataclass(frozen=True)
class Confirmation:
    message_id: str
    internal_ms: int
    url: str


def redact(text: str) -> str:
    for secret in _SECRETS:
        text = text.replace(secret, "***")
    return text


def log(message: str) -> None:
    stamp = datetime.now(UTC).strftime("%H:%M:%SZ")
    print(f"[pypi-delete {stamp}] {redact(message)}", file=sys.stderr, flush=True)


def iso(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class CsrfParser(HTMLParser):
    """Mirror of ``pypi_cleanup.CsfrParser``: the CSRF token of the form posting to ``target``.

    ``contains_input`` further requires that form to carry an input of that name, which is how
    the release page's delete form is told apart from the interstitial PyPI serves instead.
    """

    def __init__(self, target: str, contains_input: str | None = None) -> None:
        super().__init__()
        self._target = target
        self._contains_input = contains_input
        self.csrf: str | None = None
        self.any_csrf: str | None = None
        self._csrf: str | None = None
        self._in_form = False
        self._input_contained = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "form":
            action = values.get("action")
            if action and (action == self._target or action.startswith(self._target)):
                self._in_form = True
            return
        if tag != "input":
            return
        if values.get("name") == "csrf_token" and self.any_csrf is None:
            self.any_csrf = values.get("value")
        if not self._in_form:
            return
        if values.get("name") == "csrf_token":
            self._csrf = values.get("value")
        if self._contains_input and values.get("name") == self._contains_input:
            self._input_contained = True

    def handle_endtag(self, tag: str) -> None:
        if tag != "form":
            return
        self._in_form = False
        if (not self._contains_input or self._input_contained) and not self.csrf:
            self.csrf = self._csrf


def form_csrf(html: str, target: str, contains_input: str | None = None) -> str | None:
    parser = CsrfParser(target, contains_input)
    parser.feed(html)
    return parser.csrf


def page_csrf(html: str, target: str) -> str | None:
    """CSRF for a login-side form, tolerating a form action that differs from the URL path."""
    parser = CsrfParser(target)
    parser.feed(html)
    return parser.csrf or parser.any_csrf


class Client:
    """One ``requests.Session`` for the whole run, so a confirmation shares the login's cookies."""

    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "sase-pypi-delete/1 (requests)"

    def url(self, path: str) -> str:
        return path if path.startswith("http") else f"{self.base}{path}"

    def path_of(self, response: requests.Response) -> str:
        parsed = urlparse(response.url)
        return parsed.path + (f"?{parsed.query}" if parsed.query else "")

    def request(
        self, method: str, target: str, *, data: dict[str, str] | None = None, referer: str | None = None
    ) -> requests.Response:
        headers = {"referer": referer} if referer else {}
        try:
            return self.session.request(
                method, self.url(target), data=data, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except requests.RequestException as exc:
            raise DriverError("http", f"{method} {urlparse(self.url(target)).path} failed: {exc}") from exc

    def checked(
        self,
        method: str,
        target: str,
        *,
        data: dict[str, str] | None = None,
        referer: str | None = None,
        allow: tuple[int, ...] = (),
    ) -> requests.Response:
        response = self.request(method, target, data=data, referer=referer)
        if response.status_code in allow:
            return response
        if response.status_code == 429 or response.status_code >= 500:
            raise DriverError("transient", f"{method} {urlparse(response.url).path} answered {response.status_code}")
        if response.status_code >= 400:
            raise DriverError("http", f"{method} {urlparse(response.url).path} answered {response.status_code}")
        return response


def load_published(client: Client, package: str) -> dict[str, int]:
    """Published versions and their byte sizes from the public JSON API (releases with files only)."""
    response = client.checked("GET", f"/pypi/{package}/json")
    try:
        releases = response.json()["releases"]
    except (ValueError, KeyError) as exc:
        raise DriverError("http", f"unreadable JSON API response for {package}: {exc}") from exc
    return {version: sum(int(f["size"]) for f in files) for version, files in releases.items() if files}


def read_list(path: Path) -> list[str]:
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        raise Refused(f"cannot read the delete list {path}: {exc}") from exc
    versions = [line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")]
    if len(versions) != len(set(versions)):
        raise Refused(f"{path} lists a version more than once")
    return versions


def parse_versions(values: list[str], what: str) -> list[Version]:
    try:
        return [Version(value) for value in values]
    except InvalidVersion as exc:
        raise Refused(f"{what} contains an invalid version: {exc}") from exc


def validate_request(requested: list[str], protected: list[str], confirm: bool) -> None:
    """Refuse before any network traffic; PyPI deletion is permanent and burns the filename."""
    if not confirm:
        raise Refused("--confirm is required: deletion is permanent and the filenames can never be reused")
    if not requested:
        raise Refused("the delete list is empty")
    listed = parse_versions(requested, "the delete list")
    if not protected:
        return
    floor = min(parse_versions(protected, "--keep-version"))
    reaching = sorted(version for version in listed if version >= floor)
    if reaching:
        raise Refused(
            f"the delete list reaches protected version {floor} or newer "
            f"({len(reaching)} versions, newest {reaching[-1]}); every deleted version must be "
            f"strictly older than every --keep-version"
        )


def login(client: Client, username: str, password: str, totp: str | None) -> None:
    """``GET`` + ``POST /account/login/``, then the TOTP step, exactly as ``pypi-cleanup`` does."""
    login_path = "/account/login/"
    page = client.checked("GET", login_path)
    csrf = page_csrf(page.text, login_path)
    if not csrf:
        raise DriverError("login_form", f"no CSRF token on {login_path}")

    response = client.checked(
        "POST",
        login_path,
        data={"csrf_token": csrf, "username": username, "password": password},
        referer=client.url(login_path),
    )
    if response.url == client.url(login_path):
        raise DriverError("bad_credentials", f"PyPI rejected the login for {username}: wrong username or password")

    if not response.url.startswith(client.url("/account/two-factor/")):
        log("password accepted; no two-factor step")
        return

    two_factor_url = response.url
    csrf = page_csrf(response.text, client.path_of(response))
    if not csrf:
        raise DriverError("login_form", "no CSRF token on the two-factor page")
    if not totp:
        raise DriverError("totp_missing", "PyPI asked for a TOTP code but none was provided")
    response = client.checked(
        "POST",
        two_factor_url,
        data={"csrf_token": csrf, "method": "totp", "totp_value": totp},
        referer=two_factor_url,
    )
    if response.url == two_factor_url:
        raise DriverError(
            "bad_totp",
            "PyPI rejected the TOTP code (invalid or expired); answer again with a fresh code",
        )
    log("password and TOTP accepted")


def release_form(client: Client, package: str, version: str) -> tuple[bool, str | None]:
    """Return ``(exists, csrf)`` for one release's management page.

    ``exists`` is False on a 404. ``csrf`` is None when the page has no delete form, which for a
    version that is published means the account's login is unconfirmed (or has lapsed).
    """
    path = f"/manage/project/{package}/release/{version}/"
    response = client.checked("GET", path, allow=(404,))
    if response.status_code == 404:
        return False, None
    return True, form_csrf(response.text, path, "confirm_delete_version")


def gog_json(gog: str, *args: str) -> Any:
    command = [gog, *args, "-j", "--no-input"]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DriverError("gmail", f"cannot run {Path(gog).name}: {exc}") from exc
    if completed.returncode != 0:
        raise DriverError("gmail", f"{Path(gog).name} {' '.join(args[:3])} failed: {completed.stderr.strip()[:300]}")
    try:
        return json.loads(completed.stdout)
    except ValueError as exc:
        raise DriverError("gmail", f"{Path(gog).name} printed unreadable JSON: {exc}") from exc


def confirmation_url(message: dict[str, Any], base_url: str) -> str | None:
    """The ``confirm-login`` link from one message's untruncated body, if it points at PyPI."""
    body = message.get("body")
    if not isinstance(body, str):
        return None
    host = urlparse(base_url).netloc
    for candidate in CONFIRM_URL.findall(body):
        if urlparse(candidate).netloc == host:
            return candidate
    return None


def find_confirmation(gog: str, base_url: str, started_ms: int, stale: set[str]) -> Confirmation | None:
    """The newest confirmation email received at or after ``started_ms``.

    Older messages are never returned: earlier attempts (other machines, other days) left
    confirmation emails behind, and opening one of those confirms nothing.
    """
    listing = gog_json(gog, "gmail", "messages", "search", CONFIRM_QUERY)
    best: Confirmation | None = None
    for entry in listing.get("messages", [])[:CONFIRM_CANDIDATES]:
        message_id = str(entry["id"])
        if message_id in stale or CONFIRM_SUBJECT not in str(entry.get("subject", "")):
            continue
        message = gog_json(gog, "gmail", "get", message_id)
        internal_ms = int(message["message"]["internalDate"])
        if internal_ms < started_ms:
            stale.add(message_id)
            continue
        url = confirmation_url(message, base_url)
        if url is not None and (best is None or internal_ms > best.internal_ms):
            best = Confirmation(message_id, internal_ms, url)
    return best


def open_in_browser(url: str) -> None:
    """Best-effort ``xdg-open``; a missing display or opener is not an error."""
    opener = shutil.which("xdg-open")
    if opener is None or not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        log("no display or xdg-open here; open the confirmation link by hand")
        return
    try:
        subprocess.Popen(
            [opener, url],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        log("asked xdg-open to open the confirmation link")
    except OSError as exc:
        log(f"xdg-open failed: {exc}")


def clear_wall(client: Client, args: argparse.Namespace, probe: str, started_ms: int, outcome: Outcome) -> None:
    """Block until ``probe``'s release page shows the delete form, or raise ``DriverError('wall')``.

    The confirmation link is opened with ``client``'s own session, so the pending login and its
    confirmation share one cookie jar. If that is not enough, the link is also printed and handed
    to ``xdg-open`` and the release page keeps being polled, so a click on this machine still
    unblocks the same live session.
    """
    outcome.wall_encountered = True
    log(
        f"WALL: {probe}'s release page has no delete form, so PyPI parked this login as an "
        "unrecognized device; looking for the confirmation email"
    )
    deadline = time.monotonic() + args.wall_timeout
    stale: set[str] = set()
    confirmation: Confirmation | None = None
    fallback_offered = False
    gmail_failed = False
    while True:
        if confirmation is None and not gmail_failed:
            try:
                confirmation = find_confirmation(args.gog, client.base, started_ms, stale)
            except DriverError as exc:
                gmail_failed = True
                log(f"cannot read the confirmation email ({exc}); waiting for a manual confirmation")
            if confirmation is not None:
                outcome.confirmation_email_at = iso(confirmation.internal_ms)
                outcome.confirmation_source = "in_session_link"
                log(f"confirmation email received {outcome.confirmation_email_at}: {confirmation.url}")
                response = client.request("GET", confirmation.url)
                log(
                    f"opened the confirmation link in-session: HTTP {response.status_code} ({client.path_of(response)})"
                )
        exists, csrf = release_form(client, args.package, probe)
        if csrf:
            if fallback_offered or confirmation is None:
                outcome.confirmation_source = "manual_link"
            log(f"wall cleared ({outcome.confirmation_source})")
            return
        if not exists:
            raise DriverError("delete_failed", f"{probe} disappeared while waiting for the confirmation")
        if confirmation is not None and not fallback_offered:
            log("the in-session confirmation did not clear the wall; open the link above on this machine")
            fallback_offered = True
            if not args.no_open:
                open_in_browser(confirmation.url)
        if time.monotonic() >= deadline:
            break
        time.sleep(args.poll_interval)

    emailed = outcome.confirmation_email_at or "none received"
    link = f" Confirmation link: {confirmation.url}" if confirmation else ""
    raise DriverError(
        "wall",
        f"login-confirmation wall not cleared within {args.wall_timeout}s (confirmation email: {emailed}).{link}",
    )


def delete_version(client: Client, package: str, version: str) -> str:
    """Delete one release; returns ``"deleted"`` or ``"already_gone"``."""
    path = f"/manage/project/{package}/release/{version}/"
    for attempt in range(1, DELETE_ATTEMPTS + 1):
        try:
            exists, csrf = release_form(client, package, version)
            if not exists:
                return "already_gone"
            if not csrf:
                raise DriverError(
                    "session_lost", f"{version}'s release page has no delete form; the session is no longer confirmed"
                )
            client.checked(
                "POST",
                path,
                data={"csrf_token": csrf, "confirm_delete_version": version},
                referer=client.url(path),
            )
            return "deleted"
        except DriverError as exc:
            if exc.kind != "transient" or attempt == DELETE_ATTEMPTS:
                raise
            log(f"{version}: {exc}; retrying ({attempt}/{DELETE_ATTEMPTS})")
            time.sleep(5 * attempt)
    raise AssertionError("unreachable")


def measure(client: Client, package: str) -> tuple[str | None, int | None]:
    """Oldest published version and total bytes now, or ``(None, None)`` if PyPI cannot be read."""
    try:
        published = load_published(client, package)
    except DriverError as exc:
        log(f"could not re-measure {package}: {exc}")
        return None, None
    if not published:
        return None, 0
    return str(min(Version(v) for v in published)), sum(published.values())


def run(args: argparse.Namespace, environ: dict[str, str]) -> dict[str, Any]:
    requested = read_list(Path(args.list))
    validate_request(requested, args.keep_version, args.confirm)

    username = args.username or environ.get(DEFAULT_USERNAME_ENV, "")
    password = environ.get(args.password_env, "")
    totp = environ.get(args.totp_env, "").strip() or None
    if not username:
        raise Refused(f"no username: pass --username or set {DEFAULT_USERNAME_ENV}")
    if not password:
        raise Refused(f"no password: set {args.password_env} in the environment (never on the command line)")
    _SECRETS.extend(secret for secret in (password, totp) if secret)

    outcome = Outcome(requested=len(requested))
    started_ms = int(time.time() * 1000)
    client = Client(args.base_url)

    published = load_published(client, args.package)
    pending = deque(version for version in requested if version in published)
    outcome.already_gone = len(requested) - len(pending)
    if len(pending) == len(published):
        raise Refused(f"the delete list would remove every published version of {args.package}")
    log(f"{len(requested)} requested: {len(pending)} still published, {outcome.already_gone} already gone")

    try:
        if pending:
            login(client, username, password, totp)
            probe_versions = list(pending)
            probe_index = 0
            while probe_index < len(probe_versions):
                probe = probe_versions[probe_index]
                exists, csrf = release_form(client, args.package, probe)
                if not exists:
                    probe_index += 1
                    continue
                if not csrf:
                    clear_wall(client, args, probe, started_ms, outcome)
                break
            for version in probe_versions[:probe_index]:
                pending.remove(version)
                outcome.already_gone += 1
            todo = list(pending)
            for index, version in enumerate(todo, 1):
                verdict = delete_version(client, args.package, version)
                pending.remove(version)
                if verdict == "deleted":
                    outcome.deleted += 1
                else:
                    outcome.already_gone += 1
                log(f"{verdict} {version} ({index}/{len(todo)})")
    except DriverError as exc:
        outcome.failed_versions = list(pending)
        return finish(client, args, outcome, "failed", exc)
    return finish(client, args, outcome, "deleted", None)


def finish(
    client: Client, args: argparse.Namespace, outcome: Outcome, status: str, error: DriverError | None
) -> dict[str, Any]:
    oldest_kept, bytes_used_after = measure(client, args.package)
    result: dict[str, Any] = {
        "status": status,
        "package": args.package,
        "requested": outcome.requested,
        "deleted": outcome.deleted,
        "already_gone": outcome.already_gone,
        "failed": len(outcome.failed_versions),
        "wall_encountered": outcome.wall_encountered,
        "confirmation_source": outcome.confirmation_source,
        "confirmation_email_at": outcome.confirmation_email_at,
        "oldest_kept": oldest_kept,
        "bytes_used_after": bytes_used_after,
    }
    if outcome.failed_versions:
        result["failed_versions"] = outcome.failed_versions[:20]
    if error is not None:
        result["error_kind"] = error.kind
        result["error"] = redact(str(error))
    return result


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument errors still honor the one-JSON-object stdout contract."""

    def error(self, message: str) -> NoReturn:
        emit({"status": "refused", "error_kind": "usage", "error": message})
        raise SystemExit(2)


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--list", required=True, help="delete list, one version per line (from pypi_retention.py plan)")
    parser.add_argument(
        "--keep-version",
        action="append",
        default=[],
        help="version that must survive (repeatable); every listed version must be strictly older than each",
    )
    parser.add_argument("--confirm", action="store_true", help="required: acknowledge that deletion is permanent")
    parser.add_argument("--username", help=f"PyPI username (default: ${DEFAULT_USERNAME_ENV})")
    parser.add_argument("--password-env", default=DEFAULT_PASSWORD_ENV, help="env var holding the password")
    parser.add_argument("--totp-env", default=DEFAULT_TOTP_ENV, help="env var holding the current TOTP code")
    parser.add_argument("--package", default=PACKAGE)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=argparse.SUPPRESS)
    parser.add_argument(
        "--gog",
        default=shutil.which("gog") or str(Path("~/bin/gog").expanduser()),
        help="gog executable used to read the confirmation email",
    )
    parser.add_argument(
        "--wall-timeout",
        type=int,
        default=DEFAULT_WALL_TIMEOUT_SECONDS,
        help="seconds to wait for the login confirmation before giving up (default: %(default)s)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="seconds between release-page and mailbox polls (default: %(default)s)",
    )
    parser.add_argument("--no-open", action="store_true", help="never try xdg-open on the confirmation link")
    return parser


def emit(result: dict[str, Any]) -> None:
    if isinstance(result.get("error"), str):
        result["error"] = redact(result["error"])
    print(json.dumps(result), flush=True)


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run(args, dict(os.environ))
    except DriverError as exc:
        log(f"{exc.kind}: {exc}")
        emit({"status": "refused" if exc.kind == "refused" else "failed", "error_kind": exc.kind, "error": str(exc)})
        return 2 if exc.kind == "refused" else 1
    except KeyboardInterrupt:
        emit({"status": "failed", "error_kind": "interrupted", "error": "interrupted"})
        return 130
    except Exception as exc:  # the one-JSON-object contract holds even for a bug here
        log(f"unexpected {type(exc).__name__}: {exc}")
        emit({"status": "failed", "error_kind": "unexpected", "error": f"{type(exc).__name__}: {exc}"})
        return 1
    if result["status"] != "deleted":
        log(f"{result.get('error_kind')}: {result.get('error')}")
    emit(result)
    return 0 if result["status"] == "deleted" else 1


if __name__ == "__main__":
    raise SystemExit(main())
