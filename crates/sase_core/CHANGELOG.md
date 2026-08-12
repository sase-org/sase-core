# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.26.1](https://github.com/sase-org/sase-core/compare/v0.26.0...v0.26.1) - 2026-08-12

### Added

- add CI-parity verification gate

## [0.26.0](https://github.com/sase-org/sase-core/compare/v0.25.0...v0.26.0) - 2026-08-12

### Added

- resolve file artifact refs in core

### Fixed

- restore green CI on sase-core

### Other

- *(core)* [**breaking**] retire the #ref/<kind> xprompt-catalog adapter

## [0.25.0](https://github.com/sase-org/sase-core/compare/v0.24.6...v0.25.0) - 2026-08-11

### Added

- *(artifact-ref)* [**breaking**] add ref contract wire types, quoted arguments, link allocator, and Referenced By block

## [0.24.6](https://github.com/sase-org/sase-core/compare/v0.24.5...v0.24.6) - 2026-08-11

### Added

- *(vcs-log)* distinguish stitch and auto commit origins

## [0.24.5](https://github.com/sase-org/sase-core/compare/v0.24.4...v0.24.5) - 2026-08-11

### Added

- *(vcs-log)* classify commit origins

## [0.24.4](https://github.com/sase-org/sase-core/compare/v0.24.3...v0.24.4) - 2026-08-11

### Added

- *(beads)* add external ref identity field

## [0.24.3](https://github.com/sase-org/sase-core/compare/v0.24.2...v0.24.3) - 2026-08-11

### Added

- add Patch PR origin to core wire

## [0.24.2](https://github.com/sase-org/sase-core/compare/v0.24.1...v0.24.2) - 2026-08-10

### Added

- expose model alias provenance in agent scans

## [0.24.1](https://github.com/sase-org/sase-core/compare/v0.24.0...v0.24.1) - 2026-08-10

### Added

- *(beads)* search observed_since corroboration evidence

## [0.24.0](https://github.com/sase-org/sase-core/compare/v0.23.1...v0.24.0) - 2026-08-10

### Added

- *(core)* [**breaking**] enforce tale size contract

### Fixed

- *(bead)* avoid stale plus-one reopens after close
- *(core-py)* allow plus-one binding signature

## [0.23.1](https://github.com/sase-org/sase-core/compare/v0.23.0...v0.23.1) - 2026-08-10

### Fixed

- *(notifications)* scope panel icons to declared tabs

## [0.23.0](https://github.com/sase-org/sase-core/compare/v0.22.0...v0.23.0) - 2026-08-09

### Added

- [**breaking**] require tale size in core plan validation

### Added

- *(plan)* [**breaking**] require tale `size` frontmatter and expose normalized plan size

## [0.22.0](https://github.com/sase-org/sase-core/compare/v0.21.3...v0.22.0) - 2026-08-09

### Added

- *(glossary)* match phrases across line breaks
- [**breaking**] drop legacy top-level glossary path from glossary_scope_paths
- *(vcs-log)* add parent ids and merge summaries

### Fixed

- *(config)* diagnose nested glossary scope

### Other

- *(glossary)* document plural alias release note
- expose extension-module feature for PyO3 crate

## [0.21.3](https://github.com/sase-org/sase-core/compare/v0.21.2...v0.21.3) - 2026-08-09

### Fixed

- *(bead)* correct regex search match semantics

## [0.21.2](https://github.com/sase-org/sase-core/compare/v0.21.1...v0.21.2) - 2026-08-09

### Added

- *(bead)* add regex search support
- *(glossary)* derive plural aliases for matching

## [0.21.1](https://github.com/sase-org/sase-core/compare/v0.21.0...v0.21.1) - 2026-08-09

### Added

- *(core)* accept canonical patch completion metadata
- *(xprompt-lsp)* add glossary semantics
- *(core)* add glossary catalog domain

### Other

- *(core)* use patch terminology across core docs and internals

## [0.21.0](https://github.com/sase-org/sase-core/compare/v0.20.1...v0.21.0) - 2026-08-08

### Added

- *(core)* [**breaking**] add reference artifact contract
- *(core)* add Patch and stitch wire contract
- [**breaking**] use singular skill xprompt references

## [0.20.1](https://github.com/sase-org/sase-core/compare/v0.20.0...v0.20.1) - 2026-08-08

### Added

- *(xprompt)* load memory notes as invokable memory xprompts

### Fixed

- drop stale dynamic memory diagnostics

## [0.20.0](https://github.com/sase-org/sase-core/compare/v0.19.3...v0.20.0) - 2026-08-08

### Added

- *(skills)* [**breaking**] define the canonical skill layout and editor contract
- *(mobile)* [**breaking**] carry declared gate inputs on the mobile wire
- *(xprompt)* add enum input type with declared choices

### Fixed

- *(xprompt)* skip the packaged skill frame template when scanning skills

## [0.19.3](https://github.com/sase-org/sase-core/compare/v0.19.2...v0.19.3) - 2026-08-07

### Added

- *(bead)* append a snooze note recording wake conditions

## [0.19.2](https://github.com/sase-org/sase-core/compare/v0.19.1...v0.19.2) - 2026-08-07

### Added

- *(notifications)* donate a per-tab icon from the newest declaring row

## [0.19.1](https://github.com/sase-org/sase-core/compare/v0.19.0...v0.19.1) - 2026-08-07

### Fixed

- *(core-py)* bind the extension module explicitly in the package init ([#89](https://github.com/sase-org/sase-core/pull/89))

## [0.19.0](https://github.com/sase-org/sase-core/compare/v0.18.5...v0.19.0) - 2026-08-07

### Fixed

- *(bead)* [**breaking**] stop a close from bricking a snoozed bead's store

## [0.18.5](https://github.com/sase-org/sase-core/compare/v0.18.4...v0.18.5) - 2026-08-07

### Added

- *(notifications)* carry a sender-declared color on each notification tab
- *(bead)* add the snoozed task-bead status with two wake conditions
- *(notifications)* make tab ownership a single-valued core rule

## [0.18.4](https://github.com/sase-org/sase-core/compare/v0.18.3...v0.18.4) - 2026-08-06

### Added

- *(plan)* reject a malformed plan header block during validation

### Fixed

- *(editor)* report the OS error behind a dropped commit-log repository

## [0.18.3](https://github.com/sase-org/sase-core/compare/v0.18.2...v0.18.3) - 2026-08-06

### Added

- *(bead)* archive close metadata instead of destroying it on reopen ([#86](https://github.com/sase-org/sase-core/pull/86))

## [0.18.2](https://github.com/sase-org/sase-core/compare/v0.18.1...v0.18.2) - 2026-08-06

### Fixed

- *(editor)* stop a slow git log from silently emptying the commit inventory

### Other

- *(host-bridge)* stop exec-ing freshly written helper scripts

## [0.18.1](https://github.com/sase-org/sase-core/compare/v0.18.0...v0.18.1) - 2026-08-05

### Fixed

- *(bead)* relocate duplicate bead ids instead of failing the merge

## [0.18.0](https://github.com/sase-org/sase-core/compare/v0.17.16...v0.18.0) - 2026-08-03

### Added

- [**breaking**] remove prompt xprompt core bindings

## [0.17.16](https://github.com/sase-org/sase-core/compare/v0.17.15...v0.17.16) - 2026-08-03

### Fixed

- *(beads)* remove abandoned prefix migration primitives

## [0.17.15](https://github.com/sase-org/sase-core/compare/v0.17.14...v0.17.15) - 2026-08-03

### Added

- *(beads)* add prefix migration primitives

### Fixed

- *(completion)* exclude sidecars from commit refs
- *(artifacts)* honor embedded offsets for calendar dates

### Other

- *(bead)* add single-pass detail read

## [0.17.14](https://github.com/sase-org/sase-core/compare/v0.17.13...v0.17.14) - 2026-08-03

### Fixed

- *(editor)* exclude sidecar commits from artifact inventory

## [0.17.13](https://github.com/sase-org/sase-core/compare/v0.17.12...v0.17.13) - 2026-08-02

### Added

- *(lsp)* describe artifact payload rows by kind and render commit bodies
- *(editor)* enumerate local commit references
- *(py)* expose artifact ref payload inventory

## [0.17.12](https://github.com/sase-org/sase-core/compare/v0.17.11...v0.17.12) - 2026-08-02

### Added

- *(editor)* support scoped at-reference payload ranking

## [0.17.11](https://github.com/sase-org/sase-core/compare/v0.17.10...v0.17.11) - 2026-08-02

### Added

- *(core)* add xprompt provenance link rewriting

### Fixed

- *(plan)* restrict header parsing to leading block

## [0.17.10](https://github.com/sase-org/sase-core/compare/v0.17.9...v0.17.10) - 2026-08-02

### Fixed

- *(stats)* correct historical runner occupancy

## [0.17.9](https://github.com/sase-org/sase-core/compare/v0.17.8...v0.17.9) - 2026-08-02

### Added

- *(stats)* derive plan and question activity from gates

## [0.17.8](https://github.com/sase-org/sase-core/compare/v0.17.7...v0.17.8) - 2026-08-01

### Added

- *(beads)* add atomic task evidence contract
- add prompt artifact contract

### Fixed

- *(core)* make store lock waits contention resilient
- *(bead)* align compact search type column

## [0.17.7](https://github.com/sase-org/sase-core/compare/v0.17.6...v0.17.7) - 2026-08-01

### Added

- *(bead)* align Rust compact list presentation

## [0.17.6](https://github.com/sase-org/sase-core/compare/v0.17.5...v0.17.6) - 2026-08-01

### Added

- *(notifications)* add mobile activity cursors

## [0.17.5](https://github.com/sase-org/sase-core/compare/v0.17.4...v0.17.5) - 2026-08-01

### Added

- *(notifications)* define canonical snooze expiry state

## [0.17.4](https://github.com/sase-org/sase-core/compare/v0.17.3...v0.17.4) - 2026-07-31

### Added

- *(bead)* support atomic multi-ID `update_issues` mutation

## [0.17.3](https://github.com/sase-org/sase-core/compare/v0.17.2...v0.17.3) - 2026-07-31

### Added

- *(beads)* resolve shorthand bead ids in core

## [0.17.2](https://github.com/sase-org/sase-core/compare/v0.17.1...v0.17.2) - 2026-07-31

### Added

- *(notifications)* support bulk mute and snooze updates

## [0.17.1](https://github.com/sase-org/sase-core/compare/v0.17.0...v0.17.1) - 2026-07-31

### Added

- preserve bead proposal attribution
- *(commit)* parse conventional commit subjects

### Fixed

- *(commit)* classify empty subject descriptions

## [0.17.0](https://github.com/sase-org/sase-core/compare/v0.16.0...v0.17.0) - 2026-07-30

### Added

- *(bead)* [**breaking**] add task beads and ready workflow

## [0.16.0](https://github.com/sase-org/sase-core/compare/v0.15.0...v0.16.0) - 2026-07-30

### Added

- *(agent-scan)* [**breaking**] preserve bounded JSON output variables

## [0.15.0](https://github.com/sase-org/sase-core/compare/v0.14.2...v0.15.0) - 2026-07-30

### Added

- *(agent-scan)* [**breaking**] preserve list-valued output variables
- *(beads)* report projection drift diagnostics
- *(bead)* [**breaking**] add convergent note append events

### Fixed

- *(bead)* satisfy clippy in the doctor reader

## [0.14.2](https://github.com/sase-org/sase-core/compare/v0.14.1...v0.14.2) - 2026-07-30

### Fixed

- *(bead)* verify repeated closes before mutation
- *(bead)* preserve the first close in event reduction

## [0.14.1](https://github.com/sase-org/sase-core/compare/v0.14.0...v0.14.1) - 2026-07-30

### Fixed

- *(bead)* preserve refs and expose doctor context

## [0.14.0](https://github.com/sase-org/sase-core/compare/v0.13.2...v0.14.0) - 2026-07-30

### Added

- add artifact references to beads
- [**breaking**] add artifact reference list APIs

## [0.13.2](https://github.com/sase-org/sase-core/compare/v0.13.1...v0.13.2) - 2026-07-30

### Added

- add artifact store lifecycle primitives

## [0.13.1](https://github.com/sase-org/sase-core/compare/v0.13.0...v0.13.1) - 2026-07-30

### Added

- add artifact consumption ledger queries

## [0.13.0](https://github.com/sase-org/sase-core/compare/v0.12.19...v0.13.0) - 2026-07-30

### Added

- [**breaking**] materialize VCS-backed artifact files

## [0.12.19](https://github.com/sase-org/sase-core/compare/v0.12.18...v0.12.19) - 2026-07-30

### Added

- *(editor)* expand cached artifact payload inventory
- *(editor)* gate file reference rows behind explicit opt-in
- *(editor)* rank agent and indexed-file completions with the fuzzy matcher

### Fixed

- *(editor)* preserve artifact reference titles in completions

## [0.12.18](https://github.com/sase-org/sase-core/compare/v0.12.17...v0.12.18) - 2026-07-30

### Added

- *(editor)* add indexed at-reference payload binding
- *(lsp)* serve server-ranked fuzzy artifact references
- *(editor)* fuzzy-match artifact reference menus
- *(editor)* add canonical fuzzy matcher

### Fixed

- *(plan)* discover bundled document corpora

## [0.12.17](https://github.com/sase-org/sase-core/compare/v0.12.16...v0.12.17) - 2026-07-30

### Added

- *(artifact-ref)* complete bead and agent page references
- *(artifact-ref)* resolve bead and agent page references
- *(artifact-ref)* add bead and agent reference grammar

## [0.12.16](https://github.com/sase-org/sase-core/compare/v0.12.15...v0.12.16) - 2026-07-30

### Added

- *(agent-scan)* preserve swarm xprompt kind

## [0.12.15](https://github.com/sase-org/sase-core/compare/v0.12.14...v0.12.15) - 2026-07-29

### Added

- *(editor)* add shared at-reference menu core
- *(py)* expose at-reference menu bindings

## [0.12.14](https://github.com/sase-org/sase-core/compare/v0.12.13...v0.12.14) - 2026-07-29

### Added

- *(artifact)* add artifact file query API

## [0.12.13](https://github.com/sase-org/sase-core/compare/v0.12.12...v0.12.13) - 2026-07-29

### Added

- *(editor)* complete artifact references in xprompt LSP

## [0.12.12](https://github.com/sase-org/sase-core/compare/v0.12.11...v0.12.12) - 2026-07-29

### Added

- add core artifact reference APIs

## [0.12.11](https://github.com/sase-org/sase-core/compare/v0.12.10...v0.12.11) - 2026-07-29

### Added

- *(stats)* aggregate xprompt usage
- *(scan)* project xprompt usage into artifact records

## [0.12.10](https://github.com/sase-org/sase-core/compare/v0.12.9...v0.12.10) - 2026-07-29

### Added

- *(plan)* support explicit document corpora

## [0.12.9](https://github.com/sase-org/sase-core/compare/v0.12.8...v0.12.9) - 2026-07-29

### Added

- *(axe)* add structured chop report contract

## [0.12.8](https://github.com/sase-org/sase-core/compare/v0.12.7...v0.12.8) - 2026-07-29

### Added

- add keyed agent name template markers

## [0.12.7](https://github.com/sase-org/sase-core/compare/v0.12.6...v0.12.7) - 2026-07-29

### Fixed

- *(editor)* restore literal placeholder completions

## [0.12.6](https://github.com/sase-org/sase-core/compare/v0.12.5...v0.12.6) - 2026-07-29

### Added

- *(lsp)* enrich model alias completions

## [0.12.5](https://github.com/sase-org/sase-core/compare/v0.12.4...v0.12.5) - 2026-07-28

### Added

- *(beads)* support atomic close notes
- *(beads)* add epic-aware idempotent preclaims
- *(plan)* add reciprocal bead header sections (sase-ai.8)

### Fixed

- *(beads)* resolve clippy lints in close-note support
- *(beads)* expose mutation changes in CLI summaries
- *(beads)* stabilize regenerated projection ordering

## [0.12.4](https://github.com/sase-org/sase-core/compare/v0.12.3...v0.12.4) - 2026-07-28

### Added

- *(plan)* support canonical parent header migration (sase-ag.5)

### Fixed

- *(plan)* ignore fenced header examples (sase-ag.4)

## [0.12.3](https://github.com/sase-org/sase-core/compare/v0.12.2...v0.12.3) - 2026-07-28

### Added

- *(plan)* add structured header block contract (sase-ag.1)

## [0.12.2](https://github.com/sase-org/sase-core/compare/v0.12.1...v0.12.2) - 2026-07-28

### Added

- validate lumberjack wait runner limits (sase-af.1)
- *(bead)* recognize the beads sidecar root in path heuristics (sase-a8.1)

### Fixed

- *(xprompt)* canonicalize project identities in catalog (sase-ac.5)

## [0.12.1](https://github.com/sase-org/sase-core/compare/v0.12.0...v0.12.1) - 2026-07-27

### Added

- *(bead)* record dependency removals (sase-a3.2)
- *(bead)* append notes atomically in core (sase-a1.3)

## [0.12.0](https://github.com/sase-org/sase-core/compare/v0.11.4...v0.12.0) - 2026-07-27

### Added

- *(bead)* [**breaking**] make descendant close sweeps explicit (sase-a1.4)
- *(beads)* expose event history replay (sase-a1.1)

## [0.11.4](https://github.com/sase-org/sase-core/compare/v0.11.3...v0.11.4) - 2026-07-27

### Added

- *(bead)* record typed close resolutions (sase-a1.2)

## [0.11.3](https://github.com/sase-org/sase-core/compare/v0.11.2...v0.11.3) - 2026-07-27

### Added

- *(bead)* show plan references alongside where they resolve (sase-9z.4)
- *(beads)* validate doctor plan references (sase-9z.5)
- *(plan)* add durable plan reference contract (sase-9z.1)

### Fixed

- *(bead)* canonicalize created plan design refs (sase-9z.3)

## [0.11.2](https://github.com/sase-org/sase-core/compare/v0.11.1...v0.11.2) - 2026-07-27

### Fixed

- *(beads)* make event merges replay-stable (sase-9x.1)

## [0.11.1](https://github.com/sase-org/sase-core/compare/v0.11.0...v0.11.1) - 2026-07-26

### Added

- *(axe)* add description summary-body grammar (sase-9w.1)

## [0.11.0](https://github.com/sase-org/sase-core/compare/v0.10.0...v0.11.0) - 2026-07-26

### Fixed

- *(beads)* [**breaking**] make store mutations atomic (sase-9v.9)

## [0.10.0](https://github.com/sase-org/sase-core/compare/v0.9.2...v0.10.0) - 2026-07-26

### Added

- *(axe)* support required config descriptions (sase-9t.1)
- *(tasks)* accept the detached task kind in the background task store (sase-9s.2)
- *(editor)* add raw placeholder transforms (sase-9q.1)
- *(editor)* [**breaking**] tag placeholder candidates with a source and accept common tags (sase-9m.1)
- *(axe_chop)* allow one template marker in each of a chop clan and member (sase-9n.1)

### Other

- *(core-py)* reap binding test temp dirs (sase-96.8.6)

## [0.9.2](https://github.com/sase-org/sase-core/compare/v0.9.1...v0.9.2) - 2026-07-25

### Added

- *(tasks)* add durable background task store (sase-95.1)

### Fixed

- *(agent-scan)* carry wait priority explicitness (sase-9k.2)

## [0.9.1](https://github.com/sase-org/sase-core/compare/v0.9.0...v0.9.1) - 2026-07-25

### Added

- add commit SHA and legacy ownership decisions (sase-92.1)
- *(bead)* add agent wait claim mutations (sase-8y.2)
- *(bead)* add claimed status wire support (sase-8y.1)
- support transaction-gated imported agent families (sase-8v.5)
- *(beads)* add atomic batch removal (sase-8x.1)
- support xsmall and xlarge phase sizes (sase-8w.1)
- *(identity)* add owner-aware relationship domain (sase-8v.1)

### Fixed

- *(telemetry)* prevent SQLite writer lock races (sase-93.6)
- *(agent-identity)* tolerate historical family names (sase-91.1)
- *(bead)* accept claimed status in compatibility schemas (sase-8y)
- *(plan)* describe phase-size alias routing (sase-8w.7.4)
- *(bead)* expose legacy size constraint migration (sase-8w.7.1)

### Other

- cover effort suffixes on alias references (sase-8z.1)

## [0.9.0](https://github.com/sase-org/sase-core/compare/v0.8.0...v0.9.0) - 2026-07-23

### Added

- *(xprompt)* compose capitalized snippet aliases (sase-8u.1)
- *(axe)* add portable runtime status classifier (sase-8t.1)
- *(config)* add exact AXE composition planning (sase-8m.1)
- *(axe)* support clan summaries in chop proposals (sase-8l.1)
- *(machine_hood)* add machine agent hood canonicalization helpers (sase-8k.2)
- persist runner limit overrides
- *(agent-stats)* add runner occupancy analytics (sase-8j.1)
- add default effort override domain APIs
- *(plan)* add document-level artifact link contract
- add SDD frontmatter link contract
- *(telemetry)* add exact-label cleanup API (sase-8g.11)
- *(agent)* carry bead IDs through repeat launches (sase-8f.2)
- *(beads)* add atomic agent launch claims (sase-8f.1)
- *(wait)* expose queue priority in scans and completions (sase-8c.1)
- *(agent-scan)* expose bead wait markers (sase-87.5)
- *(plan)* expose managed bead links (sase-88.1)
- *(beads)* support delegated phase scheduling (sase-87.1)
- *(agent-scan)* expose parent epic plan references
- *(bead)* support phase sizing and nested cascades (sase-7z.2)
- *(plan)* validate phase sizing and parent beads (sase-7z.1)
- *(agent-clans)* resolve clan summaries (sase-7r.1)
- *(axe)* add clan-aware chop contracts (sase-7q.1)
- *(axe)* validate log rotation temp age (sase-7p.1)
- *(editor)* [**breaking**] move family and tribe grammar onto %id (sase-7o.3)
- *(agent-launch)* [**breaking**] adopt id directive grammar (sase-7n.1)
- *(agent-tribes)* [**breaking**] canonicalize tribe wire contracts (sase-7j.1)
- *(axe)* support releasing chop once-per keys (sase-7i.1)
- *(editor)* complete grouped agent references in the LSP (sase-7h.1)
- *(editor)* [**breaking**] use bare plus for project completion
- *(agent-cleanup)* add clan planning scope (sase-74.1)
- *(axe)* add per-run tokens to chop agent names

### Fixed

- *(stats)* validate open runner occupancy
- *(notifications)* reap stale atomic temp files (sase-8g.10)
- *(beads)* reconcile concurrent event streams (sase-8g.7)
- *(agent-scan)* retain runner wait priority (sase-8g.3)
- *(agent-scan)* preserve clan context in bounded snapshots
- *(axe)* validate restart backoff configuration (sase-7p.2)
- *(editor)* align clan and tribe xprompt directives
- recognize adjacent inline literals during launch planning

### Changed

- *(editor)* [**breaking**] move family and tribe identity grammar onto `%id` keyword arguments (sase-7o.3)

### Fixed

- *(editor)* align xprompt clan and tribe directives across diagnostics, completion, hover, and snippets

## [0.8.0](https://github.com/sase-org/sase-core/compare/v0.7.0...v0.8.0) - 2026-07-19

### Added

- *(agent-stats)* add project and ChangeSpec work rollups (sase-70.2)
- *(agent-stats)* aggregate durable activity logs (sase-6y.2)
- *(agent-stats)* add run statistics aggregation (sase-6y.1)
- add axe chop domain engine (sase-6v.1)
- resolve clan-level tribe metadata

## [0.7.0](https://github.com/sase-org/sase-core/compare/v0.6.0...v0.7.0) - 2026-07-18

### Added

- *(bead)* expose total authored phase count (sase-6q.1)
- *(mobile)* [**breaking**] expose generic gate branches (sase-6p.5)
- *(bead)* [**breaking**] suffix epic land agent names (sase-6n.5)
- *(runtime)* aggregate clan wall-clock runtime (sase-6n.1)

## [0.6.0](https://github.com/sase-org/sase-core/compare/v0.5.0...v0.6.0) - 2026-07-17

### Added

- *(lsp)* complete repeatable agent arguments (sase-6m.3)
- *(xprompt)* add repeatable input metadata (sase-6m.1)
- *(telemetry)* add SQLite metric store and queries (sase-6k.1)
- *(plan)* guide phase description authoring
- *(agent-scan)* add bounded artifact index deletion (sase-6j.3)
- *(notifications)* add custom gate wire support (sase-6i.1)
- *(editor)* expose family directive completions (sase-6g)
- *(cleanup)* cascade parallel family members (sase-6g.4)
- *(agent-scan)* expose parallel family membership (sase-6g.2)
- *(notifications)* add typed epic approval projection (sase-6e.2)
- *(agent-scan)* [**breaking**] drop custom role metadata (sase-6e.1)
- align catalog with canonical SASE paths (sase-6d.4)
- add structured commit footer API
- define canonical SASE content layout contract (sase-6d.1)

### Fixed

- *(editor)* describe auto arguments as gate-owned (sase-6e)

## [0.5.0](https://github.com/sase-org/sase-core/compare/v0.4.1...v0.5.0) - 2026-07-16

### Added

- *(editor)* add placeholder completion support (sase-6b.1)
- [**breaking**] require titles for all plans
- *(agent-scan)* expose agent output paths
- *(agent-scan)* expose plan commit state

## [0.4.1](https://github.com/sase-org/sase-core/compare/v0.4.0...v0.4.1) - 2026-07-15

### Fixed

- *(notifications)* match root agent identities (sase-63.1)

## [0.4.0](https://github.com/sase-org/sase-core/compare/v0.3.4...v0.4.0) - 2026-07-14

### Added

- *(plan)* add strict frontmatter validation (sase-61.1)
- *(projects)* classify projects and canonicalize lifecycle (sase-5w.1)
- [**breaking**] add conditional separators to agent auto IDs
- *(agent-scan)* project runner slot waiting fields (sase-5u.3)
- plan held workspace release on agent dismissal
- *(beads)* expand fast CLI mutations
- *(plan)* [**breaking**] retire legacy plan directories
- *(plan)* discover tiered plans in canonical directory
- add gpt-5.6 model catalog support
- [**breaking**] remove legend-tier core support
- *(vcs)* add remote commit presence classification
- *(core)* add unified VCS commit-log parser and aggregator
- *(bead)* merge event streams for conflict resolution
- *(editor)* add VCS ref completion core (sase-5i.4)
- *(lsp)* complete VCS repo slash completions (sase-5h.5)
- *(editor)* add VCS repo completion context (sase-5h.4)
- rename ChangeSpec wire review field

### Fixed

- *(bead)* merge event streams without violating strict total order
- *(prompt-stash)* bound shared lock acquisition
- *(query)* match ChangeSpecs by configured project name
- align VCS ref completion vectors (sase-5i)
- preserve repo completion spacing before final newline (sase-5h.6)

### Other

- *(bead)* address Rust 1.97 clippy lints ([#20](https://github.com/sase-org/sase-core/pull/20))
- update GPT-5.6 SOL model fixtures
- Format with `cargo fmt`

## [0.3.4](https://github.com/sase-org/sase-core/compare/v0.3.3...v0.3.4) - 2026-07-06

### Added

- scan completion video paths

### Fixed

- allow explicit child cleanup planning

## [0.3.3](https://github.com/sase-org/sase-core/compare/v0.3.2...v0.3.3) - 2026-07-06

### Added

- *(agent-scan)* carry custom role metadata (sase-5g.9)
- add launch approval mobile action contract (sase-5g.7)

## [0.3.2](https://github.com/sase-org/sase-core/compare/v0.3.1...v0.3.2) - 2026-07-06

### Added

- resolve agent family parents (sase-5f.3)

## [0.3.1](https://github.com/sase-org/sase-core/compare/v0.3.0...v0.3.1) - 2026-06-30

### Fixed

- *(editor)* keep leading @ in model completions

## [0.3.0](https://github.com/sase-org/sase-core/compare/v0.2.0...v0.3.0) - 2026-06-29

### Added

- add project display names to specs
- *(editor)* make %e an alias for %effort
- *(editor)* [**breaking**] remove %edit directive metadata and alias
- [**breaking**] move time waits under the wait directive
- [**breaking**] unify auto approval directive
- add prompt stash pin persistence

### Other

- cover directive argument completions

## [0.2.0](https://github.com/sase-org/sase-core/compare/v0.1.4...v0.2.0) - 2026-06-24

### Added

- *(editor)* [**breaking**] add %tale directive and repurpose %plan for plan auto-approval (sase-56.1)
- support directive value fanout
- *(agent-scan)* project reasoning_effort through the scan wire (sase-55.4)
- *(editor)* mirror reasoning-effort vocabulary in directive grammar (sase-55.5)
- [**breaking**] remove legacy multi-model directives
- *(config)* add Rust core config backend (sase-54.1)
- delete dismissed agent groups
- support snippet reference syntax
- *(lsp)* advertise %{A | B} alt shorthand in directive surfaces (sase-52.5)
- *(agent_launch)* support %{...} alt fan-out shorthand (sase-52.1)
- *(xprompt-lsp)* carry PR context through VCS completions
- *(editor)* open VCS project completion on bare `+` at prompt start
- [**breaking**] require #+ for VCS project completions
- *(editor)* support log_skill_use xprompt frontmatter field
- *(editor)* add vcs_project completion context, builder, and transform (sase-4z.3)
- *(bead-search)* return matches newest-first
- *(xprompt-lsp)* highlight prompt separators with semantic tokens
- *(plan)* add core plan search engine (sase-4x.2)
- *(plan)* add core plan model + discovery read layer (sase-4x.1)
- *(plan)* expose plan search via PyO3 binding (sase-4x.3)

### Fixed

- *(agent_launch)* align directive aliases with the shared registry (sase-56)
- *(agent_scan)* bump artifact-index schema to v6 for reasoning_effort
- *(agent-scan)* preserve linked repo metadata
- *(editor)* replace VCS tag at EOF during `#+` completion
- collapse empty alt whitespace
- correlate repeated alt branch names
- preserve blank lines in VCS completion edits

### Other

- format directive completion assertion
- satisfy Clippy 1.96 lints in core crate
- Revert "feat(xprompt-lsp): highlight prompt separators with semantic tokens"

## [0.1.4](https://github.com/sase-org/sase-core/compare/v0.1.3...v0.1.4) - 2026-06-18

### Fixed

- *(bead)* improve search highlight contrast

## [0.1.3](https://github.com/sase-org/sase-core/compare/v0.1.2...v0.1.3) - 2026-06-18

### Added

- *(beads)* add core bead search CLI (sase-4w.2)
- *(beads)* add core bead search engine (sase-4w.1)
- *(editor)* add frontmatter schema & validation API (sase-4r.1)
- *(prompt-stash)* add prompt-stash store module and Python bindings (sase-4q.1)
- *(core)* [**breaking**] remove the episode module and PyO3 bindings
- *(agent_scan)* carry repeat-stop metadata through the scan wire
- add sharded agent artifact layout

### Fixed

- *(xprompt)* classify markdown prompts as inline refs
- *(agent-scan)* hide abandoned terminalized agents
- *(agent-index)* expose metadata and status helpers
- quarantine corrupt artifact index sidecars

### Other

- drain stale active artifact rows
- *(agent-scan)* query related artifact dirs from index

## [0.1.2](https://github.com/sase-org/sase-core/compare/v0.1.1...v0.1.2) - 2026-06-09

### Added

- preserve prompt previews in agent group archive wire

### Fixed

- expose agent template namespaces from core

## [0.1.1](https://github.com/sase-org/sase-core/releases/tag/v0.1.1) - 2026-06-08

### Added

- add agent name template primitives (sase-4g.1)
- add exact artifact dir scanner (sase-4f.2)
- add recent dismissed group archive APIs
- add ProjectSpec alias contract (sase-4c.1)
- scan agent output variables (sase-4a.2)
- support sibling project state
- consolidate project lifecycle inactive state
- honor lifecycle filters in agent artifact scans (sase-49.4)
- add project lifecycle core contract (sase-49.1)
- add episode v2 wire contract (sase-48.1)
- add frontmatter field hover docs
- gate xprompt LSP markdown documents
- add saved agent group names to archive
- add saved agent group archive backend (sase-47.1)
- add episode wire schema (sase-45.1)
- add notification tags to core wire contracts (sase-43.1)
- support xprompt input descriptions in core (sase-3w.3)
- validate xprompt frontmatter fields
- lint invalid xprompt input types
- add active limit to agent index query (sase-3t.1)
- persist dismissed agent visibility in index (sase-3s.1)
- expose agent family fields in scan metadata (sase-3r.2)
- support parent-scoped agent index hydration (sase-3s.5)
- add visibility-aware index query and dismissed-agent sidecar (sase-3r.2)
- back bead mutations with event streams (sase-3n.3)
- read bead stores from event logs (sase-3n.2)
- add bead event reducer fixtures (sase-3n.1)
- revert sase-3e core daemon surfaces
- propagate workflow hidden state
- add ACE agent snapshot daemon read (sase-3i.4)
- compact notification projection reads (sase-3i.3)
- add projection backup and restore RPC support (sase-3e.10.3)
- extend provider host capabilities for routed calls (sase-3e.8.6)
- advertise VCS workspace host capabilities (sase-3e.8.5)
- enforce provider host manifest policy in daemon (sase-3e.8.4)
- add daemon provider host manager (sase-3e.8.3)
- add provider host IPC contract (sase-3e.8.2)
- add scheduler health contracts (sase-3e.7.8)
- add daemon scheduler queue skeleton (sase-3e.7.2)
- add agent lifecycle read projections
- add workflow daemon write surfaces (sase-3e.6.7)
- add daemon mutation write surfaces (sase-3e.6.6)
- add local daemon write contract scaffolding (sase-3e.6.1)
- add local daemon projection read contract (sase-3e.5.1)
- finish sase-3e.4 shadow indexers
- add indexer runtime foundation (sase-3e.4.1)
- add projection rebuild maintenance APIs (sase-3e.2.7)
- add agent projection storage (sase-3e.1.3)
- add changespec and notification projections (sase-3e.1.2)
- add projection event store core (sase-3e.1.1)
- remove merged bead read exports (sase-3c.3)
- allocate bead IDs from current store (sase-3c.2)
- restore legacy dismissed bundle writer (sase-3b.2)
- add Rust agent archive backend (sase-37.9)
- remove ChangeSpec test targets from core
- add counts-only notification append and rewrite APIs (sase-35.2)
- migrate Rust core to canonical .sase project spec extension (sase-33.4)
- Bulk commit of sase-33 left-over work
- add `TALE DONE` to agent cleanup `DISMISSABLE_STATUSES`
- add DismissAgentCompletions notification store primitive (sase-2v.1)
- add pending_question.json marker to agent scan wire
- Add error classifier and counts to notification wire
- Bulk commit of %time / %group / %model (bead integration) work
- add bead work preclaim mutation
- carry PDF activity in agent scan wire
- remove KICKSTART from core changespec wire
- add native editor snippet catalog fallback (sase-2f.5)
- add LSP snippet completions (sase-2f.3)
- add editor snippet helper bridge (sase-2f.2)
- stop cleanup planner rename intents (sase-2e.2)
- validate xprompt arguments in LSP diagnostics
- add precise xprompt definition ranges
- add xprompt editor definition resolver
- carry xprompt definition path through editor catalog
- load xprompt catalog in Rust
- add rich xprompt LSP editor features
- add xprompt editor analyzer core APIs
- add shared pending action state store
- add mobile notification gateway read endpoints
- add mobile notification action wire contract (sase-26.2.1)
- remove unified artifact graph core
- add batched artifact summary contract
- add SQL-backed artifact search
- add paged artifact detail contract
- classify artifact files during ingestion
- add artifact file type query semantics
- ingest workflow relationship metadata
- harden artifact migration diagnostics
- reconcile unified artifact source links
- add artifact incremental rebuild cleanup
- ingest agent thoughts into artifact graph (sase-23.2.5)
- ingest artifact graph sources
- add artifact ingestion path framework
- add artifact graph exports (sase-23.1.3)
- add artifact graph query APIs
- add artifact mutation operations (sase-23.1.2)
- add artifact graph wire schema
- add bounded agent artifact scans
- add persistent agent artifact index
- name epic land agents by bead id
- Add bead tier metadata
- add alt ids to launch fanout slots
- add Rust bead CLI planner
- add Rust bead mutation transactions
- add bead epic work planner
- add bead read bindings
- add bead storage wire contract
- add Rust agent launch fanout planner
- add launch timestamp batch allocation
- add Rust launch preparation binding
- add Rust workspace claim planning
- add agent launch wire skeletons
- scan epic start metadata
- add Rust agent compose core
- add persistent query corpus core
- expose notification store PyO3 bindings
- add Rust notification store core (sase-1n.2)
- surface markdown PDF paths in agent scan wire
- add cleanup execution helpers
- add agent cleanup side-effect intents
- add Rust agent cleanup planner
- Phase 5C — pure-Rust Git query parsers and PyO3 bindings (sase-1a.3)
- Finish work for sase-19.3
- Phase 4C — pure-Rust status state machine and PyO3 bindings (sase-19.3)
- Phase 3B — pure-Rust artifact-scan snapshot scanner (sase-18.2)
- *(query)* Phase 2C pure-Rust query evaluator and batch API (sase-17.3)
- *(query)* Phase 2B pure-Rust query tokenizer and parser (sase-17.2)
- Phase 1E — direct-parser benchmark + workflow docs (sase-16.5)
- Phase 1C — section parser parity (sase-16.3)
- Phase 1B — minimal full-file parser skeleton (sase-16.2)
- Phase 1A — Rust workspace and wire types (sase-16.1)

### Fixed

- allow signed episode importance factors (sase-48.5)
- validate markdown-local xprompts in LSP
- accept xprompts frontmatter field
- include tags in saved group wire
- honor memory long files in xprompt diagnostics
- refresh stale agent artifact index rows (sase-3u.1)
- self-heal stale rows during agent artifact index query
- rebuild corrupt agent artifact indexes
- stop treating anonymous workflows as hidden in artifact index
- enforce visible inbox index semantics (sase-3t.3)
- scan question session metadata
- tighten agent artifact inbox predicate (sase-3s.2)
- *(notifications)* make rewrite merge concurrent appends
- *(bead/events)* satisfy clippy clone_on_copy and unnecessary_sort_by lints
- stabilize workflow task projection writes
- classify completed waits as running
- satisfy current clippy archive checks
- classify pre-run agent records as starting (sase-38.5)
- add count-only notification state updates
- preserve image paths in agent scan wire
- preserve xprompt definitions in LSP catalogs
- load plugin xprompts in Rust LSP catalog
- carry agent meta tags in scan wire
- enforce artifact directory invariants
- enforce artifact binding request parity
- harden artifact graph primitive coverage (sase-23.6.1)
- wait on all epic phase agents in planner
- use underscore dismissed-name collision suffixes
- enrich running claims from artifact metadata
- align notification snapshot counts with unread badges (sase-1n)
- carry workspace_dir in agent scan markers
- Missing PLAN "Timestamps:" agent metadata field
- ChangeSpec timestamp parsing crash

### Other

- cover flow-style input descriptions
- Add tier1_active_query_is_bounded_to_newest_incomplete_rows test
- cover agent family scan metadata (sase-3r.5)
- Revert "feat: add visibility-aware index query and dismissed-agent sidecar (sase-3r.2)"
- Revert "chore: restore Rust check formatting"
- Revert "fix: tighten agent artifact inbox predicate (sase-3s.2)"
- Revert "feat: support parent-scoped agent index hydration (sase-3s.5)"
- restore Rust check formatting
- rustfmt bead/events validate chain
- cover stale projection orphan diagnostics (sase-3n.3)
- Revert "chore: satisfy current clippy lints (sase-3i.2)"
- Revert "feat: compact notification projection reads (sase-3i.3)"
- Revert "feat: add ACE agent snapshot daemon read (sase-3i.4)"
- satisfy current clippy lints (sase-3i.2)
- apply projection lint fixes
- Revert "feat: add projection event store core (sase-3e.1.1)"
- Revert "feat: add changespec and notification projections (sase-3e.1.2)"
- Revert "feat: add agent projection storage (sase-3e.1.3)"
- remove config workflow support from Rust xprompt catalog (sase-34.4)
- silence `field_reassign_with_default` in mobile notification test
- apply rustfmt to notifications import blocks
- remove cleanup rename wire fields (sase-2e.6)
- lift helper host bridge into core
- add artifact ingestion targeted fixture coverage
- simplify artifact ingestion helpers
- fix bead read clippy warning
- update bead storage path fixtures
- remove agent compose rust core
- pin notification store contract fixture (sase-1n.2)
- Revert "feat: Finish work for sase-19.3"
