# PyPI storage retention for `sase-core-rs`

PyPI caps a project's total size at 10 GB. Measured against the JSON API, the cap trips at **10 GiB = 10,737,418,240
bytes** of summed file sizes (not 10e9), and it trips mid-upload: PyPI answers `400 Project size too large` for the
first file that does not fit. That is how `0.34.48` ended up with only three of its five files. Use 10 GiB as the limit
in any headroom arithmetic. The workflow's `PYPI_PROJECT_LIMIT_BYTES` (top of `release-plz.yml`) is the value CI
enforces, and `PROJECT_LIMIT_BYTES` in `.github/scripts/pypi_retention.py` is the copy the retention tool uses; a unit
test fails if they differ, so if PyPI grants a size increase, change both.

A release is five files (macOS universal2, Windows, Linux x86_64, Linux aarch64 wheels, and the sdist) and currently
weighs about 75 MB, so a project holds roughly 140 releases when nothing else is kept.

## Runway

Measured 2026-10-19 from `uv run --script .github/scripts/pypi_retention.py verify`:

```text
sase-core-rs: 53 releases, 265 files, 4.39 GB
  used 4.39 GB (40.9% of 10 GiB), free 6.35 GB
  average newest-10 release 105.1 MB -> ~60 more releases
    runway at 4.32 releases/day: ~14 days
    runway at 1 releases/day: ~60 days
```

That is 4.09 GiB of the 10 GiB cap, 53 complete releases, about 105 MB for a newest-10 release, and about 60 days of
runway at the current one-release-a-day cadence. The 50 GiB size request
[pypi/support#12339](https://github.com/pypi/support/issues/12339) has been open since 2026-09-19; do not file a second
request. If PyPI grants it, change both `PYPI_PROJECT_LIMIT_BYTES` in `.github/workflows/release-plz.yml` and
`PROJECT_LIMIT_BYTES` in `.github/scripts/pypi_retention.py`. `test_pypi_quota.py` keeps those two constants equal.

`verify --min-runway-days D` (default 30) exits non-zero when the projected runway at one release a day falls below D.
`.github/workflows/pypi-headroom.yml` runs that check every Monday. **Live alert path:** a red scheduled run is the
alert, because ci_watch watches sase-core. The same job also tries to open or update one sase-org/sase issue titled
`[pypi-headroom] sase-core-rs runway below threshold` (label `pypi-headroom`) with `SASE_RELEASE_TOKEN`, so the sase issue
mirror can turn it into a task bead. If that token cannot write issues on sase, the red run still stands. The workflow
never deletes anything and never proposes a deletion.

## Deletion is irreversible and human-gated

- PyPI **permanently burns** a deleted version and its filenames. A deleted version can never be uploaded again.
- PyPI has no delete API and upload tokens are upload-only. `pypi-cleanup` and `.github/scripts/pypi_delete.py` both
  delete by scraping the web login form, so they need the account password and a live TOTP code. No agent or workflow
  can supply either; never automate this. `pypi_delete.py` only ever runs because a human answered a gate (or typed the
  command) with a fresh TOTP.
- CI's job is to make deletion rare (release cadence, wheel size) and to fail early and legibly when headroom runs low
  (the pre-flight guard in the `publish` job), not to delete anything.

## Tools

`.github/scripts/pypi_retention.py` is a `uv run --script` (it depends on `packaging` and imports the shared
`.github/scripts/pypi_release_files.py` parser). It owns everything around the deletion but never deletes anything
itself:

| Subcommand | Purpose                                                                                              |
| ---------- | ---------------------------------------------------------------------------------------------------- |
| `plan`     | Derive the delete list from live consumers and PyPI data, retaining the newest 30 and explicit pins. |
| `check`    | Refuse a list containing a protected version or distribution file; plan checks its own output too.   |
| `regex`    | Turn a delete list into the exact `--version-regex` argument, anchored at both ends.                 |
| `compare`  | Check that a `pypi-cleanup --query-only` log selects exactly the delete list.                        |
| `verify`   | Re-measure the project and assert the outcome (under the limit, versions gone/kept, `--min-runway-days` default 30). |

`.github/scripts/pypi_delete.py` is the deletion driver, a `uv run --script` (`packaging`, `requests`). It imports the
retention module and recomputes the live protected set before reading credentials or logging in. It reimplements the
small login/delete flow `pypi-cleanup@0.1.10` performs, with one difference that matters: it keeps its HTTP session
alive across PyPI's login-confirmation wall (see Troubleshooting) instead of exiting when it hits it. It deletes exactly
the list it is given and refuses to run when

- `--confirm` is absent,
- the list is empty, or would remove every published version, or
- any listed version is equal to or newer than a `--keep-version` (repeat it for every version a human explicitly pins),
- the list touches the live protected set, or that set cannot be computed (`error_kind: protected`, exit 2).

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

1. **Review the automatic protected set.** `plan` and `check` read each release's own PyPI metadata for the newest ten
   non-yanked releases of `sase`, `sase-github`, `sase-telegram` and `sase-research-artifacts`. For bounded core
   requirements they protect every published matching version; for unbounded requirements they protect the newest
   complete match. Extras and environment markers are handled conservatively. They also protect the windows of the
   newest two `sase` releases with a complete compatible core, the exact core pins in `pyproject.toml` at every public
   `sase` `v*` tag newer than the newest `sase` release on PyPI, the newest 30 core releases, and every
   `--keep-version`. The tag source uses `git ls-remote --tags` and anonymous raw GitHub fetches. A tag's exact
   `sase-core-rs==X.Y.Z` pin protects that published core version with a reason naming the `sase` tag; old window
   specifiers are ignored here because the consumer rules above already protect supported windows. If the tag list or
   any relevant tag's `pyproject.toml` cannot be read or parsed, planning and checking fail closed. Each protected
   version prints its reasons and every file that version currently holds. `ALREADY UNRESOLVABLE` entries name
   historical releases whose windows are empty; they are reported for the core-backfill runbook and do not block a
   reclaim.

   Check any proposed or previously saved list immediately before review:

   ```bash
   uv run --script .github/scripts/pypi_retention.py check --list delete-list.txt
   ```

   `check` accepts version names and individual distribution filenames. Missing or malformed PyPI evidence, or a
   missing expected-file set, fails closed. A `==X` requirement that is meant to **fail** in a negative test is still
   conservatively protected when it appears in a supported published release.
2. **Generate the list from live data**, never by hand:

   ```bash
   uv run --script .github/scripts/pypi_retention.py plan --keep 30 --output delete-list.txt
   ```

   `plan` checks the generated list against the same protected set before writing it. Review the printed consumer
   reasons, keep/delete counts, sizes, and projected headroom, then have the list reviewed.

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
   newest ten releases, and fails if any listed version is still published, the project is at or over the limit, or the
   projected runway at one release a day is below `--min-runway-days` (default 30).

## Pre-flight quota guard

The `publish` job runs `.github/scripts/pypi_quota.py check dist` before `Publish to PyPI`. It sums the project's current
size from the PyPI JSON API, adds the bytes of the `dist/` files PyPI does not already hold (a heal of a partial release
is only charged for the files it will actually upload, since `skip-existing` skips the rest), and compares the total to
`PYPI_PROJECT_LIMIT_BYTES`.

- **Would not fit:** the job fails before uploading anything, with the current size, incoming bytes, limit, and overflow
  in an `::error::` annotation, so no partial release is left behind. The fix is to reclaim storage with the retention
  workflow above (human-gated), then re-run; the publish gate sees the version is still unpublished and retries.
- **Fits:** every publish appends bytes used, bytes free after the upload, and roughly how many more releases fit (at the
  average of the newest ten) to the job summary, so headroom is visible as a gauge instead of a cliff.
- **PyPI API unreachable or malformed:** the guard logs a `::warning::` and lets the upload proceed. It exists to give a
  better error, and must not itself fail a good release.

Try it against the live project, sizing `dist/` like a real release:

```bash
PYPI_PROJECT_LIMIT_BYTES=10737418240 python3 .github/scripts/pypi_quota.py check dist
```

## Release cadence

`Release-plz` cuts **one release a day**, not one per master push. Merging the release PR on every push produced 4.32
releases a day (272 in 63 days) at about 75 MB each, roughly 324 MB of quota a day, which is what filled the project.
Reclaiming storage alone buys only weeks at that rate, so the cadence is bounded too: at one release a day the projected
runway after a keep-30 reclaim is about 105 days instead of 24.

Which trigger does what to the release PR:

| Trigger                               | Updates release PR | Merges it (cuts a release) |
| ------------------------------------- | ------------------ | -------------------------- |
| push to `master`                      | yes                | **no**                     |
| `23 */6 * * *` schedule (the heal)    | yes                | **no**                     |
| `41 7 * * *` schedule (the daily cut) | yes                | **yes**                    |
| `workflow_dispatch`, `dry_run=false`  | yes                | **yes**                    |
| `workflow_dispatch`, `dry_run=true`   | no                 | no                         |

Only `release-plz-merge` is gated. A push still runs `release-plz-release`, `release-plz-pr` and the `publish-plan`
gate, so the release PR keeps accumulating commits and a tagged-but-incomplete version still heals on the next push or
six-hourly run. The daily cron string appears in `on.schedule`, in the merge job's `if` and in its concurrency group; a
unit test (`test_release_cadence.py`) keeps those copies identical, so change all three together. It fires at 07:41 UTC,
off the top of the hour that GitHub congests and clear of the heal's `:23` slots. GitHub may still delay a scheduled
run.

**Cutting an urgent release** (a core fix a floor ratchet is waiting on) does not have to wait for the next cut.
Dispatch the workflow with `dry_run=false`; `dry_run` defaults to true, and a dry-run dispatch skips the release PR and
the merge entirely:

```bash
gh workflow run release-plz.yml --repo sase-org/sase-core -f dry_run=false
```

That updates the release PR, waits for its checks, and squash-merges it. The merge is a push to `master`, and that run
tags the version, then builds and publishes it. If the push starts no run, the next six-hourly heal tags and publishes
it. Keep manual cuts rare: each one is about 75 MB of quota, and every publish reports the headroom left in its job
summary.

Every merge safety guard on the release PR is unchanged (base branch, author, head-branch timestamp shape, title shape
and body footer), because `master` is unprotected and the merge job must never touch an arbitrary open PR.

## Healing a partial release

A release that exists on PyPI with only some of its five files is a **partial release**: a mid-upload quota rejection
leaves one behind (that is what happened to `0.34.48`, which lost its `win_amd64` wheel and sdist). PyPI answers HTTP
200 for such a version, so the `publish-plan` job does not ask whether the version exists; it asks whether the release
holds every distribution named in `EXPECTED_DIST_SUFFIXES` (the one list at the top of `release-plz.yml`) with none
yanked, through `.github/scripts/pypi_release_files.py status`. Anything short of that reads as unpublished and the
build/publish jobs run. `publish` passes `skip-existing: true`, so a heal only ever adds files PyPI does not already
have.

- The expected set lives in the workflow and nowhere else. When the build matrix changes, update
  `EXPECTED_DIST_SUFFIXES` in the same commit: the `publish` job compares `dist/` to it one-to-one and fails before
  uploading anything if a build is missing from either side.
- The automatic gate only examines the current workspace version. It never republishes history, so an older partial
  release is not healed automatically.
- A yanked file does not count as present, but it is reported as `yanked` rather than `partial`, and `yanked` does
  **not** trigger a build. PyPI never accepts a re-upload of a yanked filename, so a rebuild could not heal it; gating
  on it would queue the whole wheel matrix on every push and every scheduled run forever, each time to publish
  nothing. The job logs a `::warning::` instead. The remedy for a yanked workspace version is to **bump the version**,
  not to rebuild it.

Check any version from a checkout (stdlib only, `absent`, `partial`, `yanked` or `complete` on stdout, the missing or
yanked entries on stderr):

```bash
EXPECTED_DIST_SUFFIXES="$(python3 -c "import yaml; print(yaml.safe_load(open('.github/workflows/release-plz.yml'))['env']['EXPECTED_DIST_SUFFIXES'])")" \
  python3 .github/scripts/pypi_release_files.py status 0.34.48
```

Heal an older partial version with the guarded manual route. It rebuilds from the existing `v<version>` tag, so nothing
newer on master can leak in, and `skip-existing` uploads only the missing files:

```bash
gh workflow run release-plz.yml --repo sase-org/sase-core \
  -f dry_run=false -f build_wheels=true -f publish_pypi=true -f expected_version=0.34.48
```

`dry_run=false` is required by the `publish` job, and it also makes this dispatch a release cut (see "Release
cadence"): the release-plz PR and merge jobs run too, so an open release PR is merged as a side effect and costs another
release of quota. Confirm the outcome afterwards by re-running `status`, which must print `complete`. PyPI's JSON API is
CDN-cached for a short while after an upload (right after the `0.34.48` heal it still listed three files for a moment),
so re-query before concluding a heal failed. Healing does not add versions: it can only add the missing files of a
version that already exists.

## Runner starvation

A job GitHub never assigned a runner to looks like this: the check-run annotation says
`The job was not acquired by Runner of type hosted even after multiple attempts`, the job concludes `cancelled` with
no steps and no runner (`runner_id: 0`, `runner_name: ""`), the run concludes `failure`, and `twine check`/`publish`
are skipped. That is what Release-plz run `40346929251` did to `v0.42.7`: only `macos universal2` went red, the other
four builds succeeded, and the version stayed unpublished for 4.5+ hours until an unrelated push healed it.

`.github/workflows/runner-starvation-retry.yml` watches the `Release-plz` and `CI` workflows for completed `failure`
runs and reruns the run once when **every** red job is starved in that sense. Mixed failures are never rerun: a job
that ran and failed may already have uploaded an artifact, and a rerun would rerun that real failure too. A cancelled
run is never rerun either: cancellation means a person or a concurrency group stopped it. The next push or the
six-hourly heal stays the backstop, so a rerun that is itself starved is left alone, never rerun a second time.

Manual remedy for one run:

```bash
gh run rerun <run id> --repo sase-org/sase-core --failed
```

Live behaviour verified against run `40346929251`: `gh run rerun --failed` does rerun a job whose conclusion was
`cancelled`, so the helper reruns with `--failed` (attempt 2 picked the macOS job straight up with a real runner).
Cross-attempt artifacts were still being verified when this shipped: attempt 2 reuses attempt 1's successful jobs, and
run-scoped artifacts should make `dist/` whole for `twine check`, but confirm on the next starvation rerun by checking
that its `twine check` step lists all five files.

## Core backfill

A heal can only top up a version PyPI already knows. When a supported consumer's window holds **no** complete version at
all — the `ALREADY UNRESOLVABLE` block that `plan` prints for sase 0.1.5 through 0.17.1, for example — the remedy is a
**backfill**: cut a new patch version from an old tag (for example `0.32.17` from `v0.32.16`) so the window has a
complete version to resolve to. Backfill is human-gated; this section documents it and no agent or workflow performs it.

1. **Confirm the window is really empty.** Run `plan` and read the `ALREADY UNRESOLVABLE` block, or check the candidate
   directly with the expected-file set from `release-plz.yml`:

   ```bash
   EXPECTED_DIST_SUFFIXES="$(python3 -c "import yaml; print(yaml.safe_load(open('.github/workflows/release-plz.yml'))['env']['EXPECTED_DIST_SUFFIXES'])")" \
     python3 .github/scripts/pypi_release_files.py status <version>
   ```

   It must not print `complete` for any version in the window.
2. **Cut the patch from the old tag.** Branch from the tag, bump `[workspace.package].version` in `Cargo.toml` (and the
   lockfile) to the new patch, commit, tag `v<new-version>`, and push the branch and the tag. The tag must carry the
   bumped version: the publish guard below refuses a `dist/` whose wheel/sdist metadata does not equal
   `Name=sase-core-rs Version=<expected_version>`, so a tag that still says the old version cannot publish. Pushing the
   tag alone triggers nothing; a backfill only ships through the guarded dispatch in step 4.
3. **Look for an open release PR first.** The dispatch in step 4 sets `dry_run=false`, which also makes it a release
   cut (see "Release cadence"): the release-plz PR and merge jobs run too, so an open release PR is merged as a side
   effect and costs another release of quota. Either let that merge happen deliberately or wait for the daily cut to
   clear it first.
4. **Dispatch the guarded manual publish from `master`:**

   ```bash
   gh workflow run release-plz.yml --repo sase-org/sase-core --ref master \
     -f dry_run=false -f build_wheels=true -f publish_pypi=true -f expected_version=0.32.17
   ```

   There is no separate approval label on this route; the guard is these four inputs together (verified against the
   workflow on 2026-10-19). The build jobs check out `build_ref=v<expected_version>`, so nothing newer on master can
   leak into the backfill. The `publish` job then requires `GITHUB_REF=refs/heads/master`, the `v<expected_version>`
   tag to exist, `dist/` to match `EXPECTED_DIST_SUFFIXES` one-to-one, and every file's metadata to equal the expected
   name and version. The quota pre-flight (`pypi_quota.py check dist`) fails before the first byte is sent when the
   upload would overflow the project limit, and `skip-existing` keeps a retried dispatch idempotent.
5. **Pay the quota cost knowingly.** A backfill is a full new release of about 75–105 MB. Each one shortens the runway
   in "Runway" above, so backfill only windows a supported sase release actually needs; the already-broken historical
   windows stay broken until they age out of the horizon.
6. **Confirm.** Re-run `status` for the new version with `EXPECTED_DIST_SUFFIXES` set as in step 1; it must print
   `complete` (re-query once first: PyPI's JSON API is CDN-cached briefly after an upload), and re-run `plan` to check
   the window left the `ALREADY UNRESOLVABLE` block.
   Also glance at the next daily release cut: the out-of-band tag must not have confused the release PR about which
   version comes next.

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
