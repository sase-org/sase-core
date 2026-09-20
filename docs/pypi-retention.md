# PyPI storage retention for `sase-core-rs`

PyPI caps a project's total size at 10 GB. Measured against the JSON API, the cap trips at **10 GiB = 10,737,418,240
bytes** of summed file sizes (not 10e9), and it trips mid-upload: PyPI answers `400 Project size too large` for the
first file that does not fit. That is how `0.34.48` ended up with only three of its five files. Use 10 GiB as the limit
in any headroom arithmetic; `PROJECT_LIMIT_BYTES` in `.github/scripts/pypi_retention.py` is the one place it lives.

A release is five files (macOS universal2, Windows, Linux x86_64, Linux aarch64 wheels, and the sdist) and currently
weighs about 75 MB, so a project holds roughly 140 releases when nothing else is kept.

## Deletion is irreversible and human-gated

- PyPI **permanently burns** a deleted version and its filenames. A deleted version can never be uploaded again.
- PyPI has no delete API and upload tokens are upload-only. `pypi-cleanup` deletes by scraping the web login form, so it
  needs the account password and a live TOTP code. No agent or workflow can supply either; never automate this.
- CI's job is to make deletion rare (release cadence, wheel size) and to fail early and legibly when headroom runs low
  (the pre-flight guard in the `publish` job), not to delete anything.

## Tool

`.github/scripts/pypi_retention.py` is a self-contained `uv run --script` (it depends only on `packaging`). It owns
everything around the deletion but never deletes anything itself:

| Subcommand | Purpose                                                                              |
| ---------- | ------------------------------------------------------------------------------------ |
| `plan`     | Derive the delete list from live data: sort by version, keep the newest N.           |
| `regex`    | Turn a delete list into the exact `--version-regex` argument, anchored at both ends. |
| `compare`  | Check that a `pypi-cleanup --query-only` log selects exactly the delete list.        |
| `verify`   | Re-measure the project and assert the outcome (under the limit, versions gone/kept). |

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

4. **Delete** (human only, with a fresh TOTP). The password comes from the environment, the TOTP is typed at the
   `Authentication code:` prompt:

   ```bash
   PYPI_CLEANUP_PASSWORD=... uvx pypi-cleanup@0.1.10 --username <you> --package sase-core-rs --do-it \
     --version-regex "$REGEX"
   ```

5. **Verify** by re-measuring, requiring the versions you meant to keep and the oldest survivor:

   ```bash
   uv run --script .github/scripts/pypi_retention.py verify --list delete-list.txt \
     --require-version <a consumer floor> --expect-oldest <oldest kept version>
   ```

   It prints bytes used, bytes free, and the approximate number of releases that still fit at the average size of the
   newest ten releases, and fails if any listed version is still published or the project is at or over the limit.

## Troubleshooting

- `ValueError: No CSFR found in /manage/project/.../release/<version>/` after a successful login means PyPI served an
  interstitial (new device / re-authentication) instead of the release management form. Nothing was deleted. Confirm the
  new device from the email PyPI sent, then rerun with a **fresh** TOTP. Replaying a stored command without re-entering
  the password and TOTP cannot work.
- A `--query-only` run that selects nothing has no `Found the following releases` block, and `compare` refuses it rather
  than treating an empty selection as a match. Either the list is already gone or the tool's log format changed.
