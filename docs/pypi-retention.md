# PyPI storage retention for `sase-core-rs`

PyPI caps a project's total size at 10 GB. Measured against the JSON API, the cap trips at **10 GiB = 10,737,418,240
bytes** of summed file sizes (not 10e9), and it trips mid-upload: PyPI answers `400 Project size too large` for the
first file that does not fit. That is how `0.34.48` ended up with only three of its five files. Use 10 GiB as the limit
in any headroom arithmetic; `PROJECT_LIMIT_BYTES` in `.github/scripts/pypi_retention.py` is the one place it lives.

A release is five files (macOS universal2, Windows, Linux x86_64, Linux aarch64 wheels, and the sdist) and currently
weighs about 75 MB, so a project holds roughly 140 releases when nothing else is kept.

## Deletion is irreversible and human-gated

- PyPI **permanently burns** a deleted version and its filenames. A deleted version can never be uploaded again.
- PyPI has no delete API and upload tokens are upload-only. `pypi-cleanup` and `.github/scripts/pypi_delete.py` both
  delete by scraping the web login form, so they need the account password and a live TOTP code. No agent or workflow
  can supply either; never automate this. `pypi_delete.py` only ever runs because a human answered a gate (or typed the
  command) with a fresh TOTP.
- CI's job is to make deletion rare (release cadence, wheel size) and to fail early and legibly when headroom runs low
  (the pre-flight guard in the `publish` job), not to delete anything.

## Tools

`.github/scripts/pypi_retention.py` is a self-contained `uv run --script` (it depends only on `packaging`). It owns
everything around the deletion but never deletes anything itself:

| Subcommand | Purpose                                                                              |
| ---------- | ------------------------------------------------------------------------------------ |
| `plan`     | Derive the delete list from live data: sort by version, keep the newest N.           |
| `regex`    | Turn a delete list into the exact `--version-regex` argument, anchored at both ends. |
| `compare`  | Check that a `pypi-cleanup --query-only` log selects exactly the delete list.        |
| `verify`   | Re-measure the project and assert the outcome (under the limit, versions gone/kept). |

`.github/scripts/pypi_delete.py` is the deletion driver, also a self-contained `uv run --script` (`packaging`,
`requests`). It reimplements the small login/delete flow `pypi-cleanup@0.1.10` performs, with one difference that
matters: it keeps its HTTP session alive across PyPI's login-confirmation wall (see Troubleshooting) instead of exiting
when it hits it. It deletes exactly the list it is given and refuses to run when

- `--confirm` is absent,
- the list is empty, or would remove every published version, or
- any listed version is equal to or newer than a `--keep-version` (repeat it for every version a consumer pins exactly).

It never prompts, because it runs from a gate with no TTY. The username is `--username` (or `PYPI_USERNAME`), the
password comes from `PYPI_CLEANUP_PASSWORD` and the current TOTP from `PYPI_TOTP` (`--password-env` / `--totp-env` name
other variables); the password is never accepted on the command line and neither secret is ever printed or written. It
prints exactly one JSON object on stdout (`status`, `requested`, `deleted`, `already_gone`, `failed`,
`wall_encountered`, `confirmation_source`, `oldest_kept`, `bytes_used_after`, and `error_kind` / `error` on failure) and
sends every diagnostic to stderr. It exits 0 only for `"status": "deleted"`; a refusal exits 2.

It tells the failures apart, because they have different fixes:

| `error_kind`      | Meaning                                       | Fix                                     |
| ----------------- | --------------------------------------------- | --------------------------------------- |
| `bad_credentials` | PyPI kept the login form after the password   | Check the username and password         |
| `bad_totp`        | PyPI kept the two-factor form after the code  | Retry with a **fresh** TOTP             |
| `wall`            | Login parked as an unrecognized device        | Confirm the emailed link (see below)    |
| `session_lost`    | A release page lost its delete form mid-run   | Rerun: the list is regenerated live     |
| `transient`       | PyPI answered 429 or 5xx three times in a row | Rerun: already-deleted versions skipped |

## Procedure

1. **Choose the keep boundary.** The default keeps the newest 30 published versions so a rollback target older than the
   current floor still exists. Before deleting, grep every consumer of the binding (`sase`, `sase-github`,
   `sase-telegram`, `sase-nvim`, `sase-research-artifacts`) for an exact `sase-core-rs==` pin or a floor below the
   boundary, including `uv.lock`. If one pins a version that would be deleted, raise the boundary or pass
   `--keep-version` for it. A `==X` pin that is meant to **fail** (a negative test) does not need protecting.
2. **Generate the list from live data**, never by hand:

   ```bash
   uv run --script .github/scripts/pypi_retention.py plan --keep 30 --output delete-list.txt
   ```

   Review the printed keep/delete counts, sizes, and the projected headroom, then have the list reviewed.

3. **Dry-run.** `--query-only` needs no credentials and prints what would be deleted. The selection must equal the list
   (use `--allow-subset` only to tolerate versions that are already gone; extra versions are always a mismatch):

   ```bash
   REGEX=$(uv run --script .github/scripts/pypi_retention.py regex --list delete-list.txt)
   uvx pypi-cleanup@0.1.10 --package sase-core-rs --query-only --version-regex "$REGEX" 2>&1 \
     | uv run --script .github/scripts/pypi_retention.py compare --list delete-list.txt
   ```

   `pypi-cleanup` matches with `re.match`, which only anchors the start; the generated regex is anchored with `^...$` so
   `0.34.1` cannot also select `0.34.10`. Stop and re-derive if `compare` reports a mismatch.

4. **Delete** (human only, with a fresh TOTP), through the driver. Regenerate the list and re-run the dry run
   immediately before, because the keep window slides with every release; never reuse a stale list. The password and
   TOTP come from the environment:

   ```bash
   PYPI_CLEANUP_PASSWORD=... PYPI_TOTP=<fresh code> \
     uv run --script .github/scripts/pypi_delete.py --username <you> --list delete-list.txt \
     --keep-version 0.34.23 --keep-version 0.34.48 --confirm
   ```

   Run it from a machine whose operator can open the confirmation link PyPI emails, and on which `gog` can read that
   mailbox (the driver looks the email up itself). A partial run is safe to resume: regenerate the list and run again,
   and versions that are already gone are skipped (`compare --allow-subset` tolerates them in the dry run).

   Upstream `uvx pypi-cleanup@0.1.10 --username <you> --package sase-core-rs --do-it --version-regex "$REGEX"` remains a
   valid fallback, but only from a machine PyPI already recognizes: it exits at the first wall and cannot recover.

5. **Verify** by re-measuring, requiring the versions you meant to keep and the oldest survivor:

   ```bash
   uv run --script .github/scripts/pypi_retention.py verify --list delete-list.txt \
     --require-version <a consumer floor> --expect-oldest <oldest kept version>
   ```

   It prints bytes used, bytes free, and the approximate number of releases that still fit at the average size of the
   newest ten releases, and fails if any listed version is still published or the project is at or over the limit.

## Troubleshooting

- `ValueError: No CSFR found in /manage/project/.../release/<version>/` after a successful login means PyPI served its
  **login-confirmation wall** instead of the release management form: a login from a device PyPI does not recognize is
  parked until the emailed link is opened. It is not a CSRF bug. Nothing was deleted, and replaying a stored command
  without re-entering the password and TOTP cannot work.

  The email says to open the link "from the same device from which you attempted to log in", and the confirmation has to
  be consumed by **the session that triggered it**. That is why the obvious recovery fails: `pypi-cleanup` exits the
  moment it hits the wall and its `requests.Session` dies with it, so a browser click afterwards confirms a login that
  no longer exists. Two attempts from another machine produced two emails and zero deletions this way. `pypi-cleanup`
  also does not detect the wall: it only checks that the post-login URL is not the login URL, so an unconfirmed session
  looks like a success and it walks on to the first release page. Upstream reads the same way —
  [issue #42](https://github.com/arcivanov/pypi-cleanup/issues/42) and
  [issue #49](https://github.com/arcivanov/pypi-cleanup/issues/49) both resolve by confirming the emailed login, and
  open PR [#48](https://github.com/arcivanov/pypi-cleanup/pull/48) exists to handle the redirect in the tool.
  **Upgrading is not a fix**: `0.1.10` is still the newest stable release, and `0.1.11.dev20260320034404` is
  byte-identical to it in `pypi_cleanup/__init__.py`.

  `pypi_delete.py` resolves it by construction. When the first release page has no delete form it waits for the
  `[PyPI] Unrecognized login to your PyPI account` email, opens the `confirm-login` link **with the same session**, and
  polls the release page (every 5 seconds, up to `--wall-timeout`, 10 minutes by default) until the delete form appears.
  It only accepts an email received at or after its own login: earlier attempts leave stale confirmation emails behind,
  and opening one of those confirms nothing. It also prints the link on stderr and, if the in-session open is not
  enough, hands it to `xdg-open`, so a click on the same machine still unblocks the same live session.
  `confirmation_source` in the result records which path cleared the wall (`not_needed`, `in_session_link`, or
  `manual_link`). The login must therefore originate from a machine whose operator can reach the emailed link; a login
  from a headless host nobody can open a browser on can never be confirmed.

  If the wall still wins, in order: install `pypi-cleanup` from upstream PR #48, which handles the redirect; or sign in
  to PyPI in a browser on the deleting machine first and clear the confirmation, so the device is already recognized
  when the driver logs in. Record whichever worked here.

- A TOTP error (`bad_totp`) is not the wall: the code was invalid or had expired. Generate it immediately before
  submitting and run again; nothing was deleted.
- A `--query-only` run that selects nothing has no `Found the following releases` block, and `compare` refuses it rather
  than treating an empty selection as a match. Either the list is already gone or the tool's log format changed.
