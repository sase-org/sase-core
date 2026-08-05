# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Other

- *(bead)* add single-pass detail read

## [0.17.13](https://github.com/sase-org/sase-core/compare/v0.17.12...v0.17.13) - 2026-08-02

### Added

- *(py)* expose artifact ref payload inventory

## [0.17.11](https://github.com/sase-org/sase-core/compare/v0.17.10...v0.17.11) - 2026-08-02

### Added

- *(core)* add xprompt provenance link rewriting

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

## [0.17.5](https://github.com/sase-org/sase-core/compare/v0.17.4...v0.17.5) - 2026-08-01

### Added

- *(notifications)* define canonical snooze expiry state

## [0.17.4](https://github.com/sase-org/sase-core/compare/v0.17.3...v0.17.4) - 2026-07-31

### Added

- *(bead)* support atomic multi-ID `update_issues` mutation

## [0.17.3](https://github.com/sase-org/sase-core/compare/v0.17.2...v0.17.3) - 2026-07-31

### Added

- *(beads)* resolve shorthand bead ids in core

## [0.17.1](https://github.com/sase-org/sase-core/compare/v0.17.0...v0.17.1) - 2026-07-31

### Added

- *(commit)* parse conventional commit subjects

## [0.17.0](https://github.com/sase-org/sase-core/compare/v0.16.0...v0.17.0) - 2026-07-30

### Added

- *(bead)* [**breaking**] add task beads and ready workflow

## [0.15.0](https://github.com/sase-org/sase-core/compare/v0.14.2...v0.15.0) - 2026-07-30

### Added

- *(beads)* report projection drift diagnostics

## [0.14.1](https://github.com/sase-org/sase-core/compare/v0.14.0...v0.14.1) - 2026-07-30

### Fixed

- *(bead)* preserve refs and expose doctor context

## [0.14.0](https://github.com/sase-org/sase-core/compare/v0.13.2...v0.14.0) - 2026-07-30

### Added

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

- *(editor)* gate file reference rows behind explicit opt-in

## [0.12.18](https://github.com/sase-org/sase-core/compare/v0.12.17...v0.12.18) - 2026-07-30

### Added

- *(editor)* add indexed at-reference payload binding

## [0.12.17](https://github.com/sase-org/sase-core/compare/v0.12.16...v0.12.17) - 2026-07-30

### Added

- *(artifact-ref)* add bead and agent reference grammar

## [0.12.15](https://github.com/sase-org/sase-core/compare/v0.12.14...v0.12.15) - 2026-07-29

### Added

- *(py)* expose at-reference menu bindings

## [0.12.14](https://github.com/sase-org/sase-core/compare/v0.12.13...v0.12.14) - 2026-07-29

### Added

- *(artifact)* add artifact file query API

## [0.12.12](https://github.com/sase-org/sase-core/compare/v0.12.11...v0.12.12) - 2026-07-29

### Added

- add core artifact reference APIs

## [0.12.11](https://github.com/sase-org/sase-core/compare/v0.12.10...v0.12.11) - 2026-07-29

### Added

- *(stats)* aggregate xprompt usage

## [0.12.10](https://github.com/sase-org/sase-core/compare/v0.12.9...v0.12.10) - 2026-07-29

### Added

- *(plan)* support explicit document corpora

## [0.12.8](https://github.com/sase-org/sase-core/compare/v0.12.7...v0.12.8) - 2026-07-29

### Added

- add keyed agent name template markers

## [0.12.5](https://github.com/sase-org/sase-core/compare/v0.12.4...v0.12.5) - 2026-07-28

### Added

- *(beads)* support atomic close notes
- *(beads)* add epic-aware idempotent preclaims
- *(plan)* add reciprocal bead header sections (sase-ai.8)

### Fixed

- *(beads)* resolve clippy lints in close-note support

## [0.12.4](https://github.com/sase-org/sase-core/compare/v0.12.3...v0.12.4) - 2026-07-28

### Added

- *(plan)* support canonical parent header migration (sase-ag.5)

## [0.12.3](https://github.com/sase-org/sase-core/compare/v0.12.2...v0.12.3) - 2026-07-28

### Added

- *(plan)* add structured header block contract (sase-ag.1)

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

## [0.11.2](https://github.com/sase-org/sase-core/compare/v0.11.1...v0.11.2) - 2026-07-27

### Fixed

- *(beads)* make event merges replay-stable (sase-9x.1)

## [0.11.1](https://github.com/sase-org/sase-core/compare/v0.11.0...v0.11.1) - 2026-07-26

### Added

- *(axe)* add description summary-body grammar (sase-9w.1)

## [0.10.0](https://github.com/sase-org/sase-core/compare/v0.9.2...v0.10.0) - 2026-07-26

### Added

- *(axe)* support required config descriptions (sase-9t.1)
- *(editor)* add raw placeholder transforms (sase-9q.1)
- *(editor)* [**breaking**] tag placeholder candidates with a source and accept common tags (sase-9m.1)

### Other

- *(core-py)* reap binding test temp dirs (sase-96.8.6)

## [0.9.2](https://github.com/sase-org/sase-core/compare/v0.9.1...v0.9.2) - 2026-07-25

### Added

- *(tasks)* add durable background task store (sase-95.1)

## [0.9.1](https://github.com/sase-org/sase-core/compare/v0.9.0...v0.9.1) - 2026-07-25

### Added

- add commit SHA and legacy ownership decisions (sase-92.1)
- *(bead)* add agent wait claim mutations (sase-8y.2)
- *(beads)* add atomic batch removal (sase-8x.1)
- *(identity)* add owner-aware relationship domain (sase-8v.1)

### Fixed

- *(agent-identity)* tolerate historical family names (sase-91.1)
- *(bead)* expose legacy size constraint migration (sase-8w.7.1)

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
- *(beads)* add atomic agent launch claims (sase-8f.1)
- *(plan)* expose managed bead links (sase-88.1)
- *(beads)* support delegated phase scheduling (sase-87.1)
- *(bead)* support phase sizing and nested cascades (sase-7z.2)
- *(plan)* validate phase sizing and parent beads (sase-7z.1)
- *(agent-clans)* resolve clan summaries (sase-7r.1)
- *(axe)* add clan-aware chop contracts (sase-7q.1)
- *(agent-tribes)* [**breaking**] canonicalize tribe wire contracts (sase-7j.1)
- *(axe)* support releasing chop once-per keys (sase-7i.1)
- *(agent-cleanup)* add clan planning scope (sase-74.1)
- *(axe)* add per-run tokens to chop agent names

### Fixed

- *(beads)* reconcile concurrent event streams (sase-8g.7)
- recognize adjacent inline literals during launch planning

### Added

- expose clan summary scan metadata and deterministic resolution (sase-7r.1)

## [0.8.0](https://github.com/sase-org/sase-core/compare/v0.7.0...v0.8.0) - 2026-07-19

### Added

- *(agent-stats)* add project and ChangeSpec work rollups (sase-70.2)
- *(agent-stats)* aggregate durable activity logs (sase-6y.2)
- *(agent-stats)* add run statistics aggregation (sase-6y.1)
- add axe chop domain engine (sase-6v.1)
- resolve clan-level tribe metadata

## [0.7.0](https://github.com/sase-org/sase-core/compare/v0.6.0...v0.7.0) - 2026-07-18

### Added

- *(runtime)* aggregate clan wall-clock runtime (sase-6n.1)

## [0.6.0](https://github.com/sase-org/sase-core/compare/v0.5.0...v0.6.0) - 2026-07-17

### Added

- *(telemetry)* add SQLite metric store and queries (sase-6k.1)
- *(plan)* guide phase description authoring
- *(agent-scan)* add bounded artifact index deletion (sase-6j.3)
- *(notifications)* add custom gate wire support (sase-6i.1)
- *(cleanup)* cascade parallel family members (sase-6g.4)
- *(notifications)* add typed epic approval projection (sase-6e.2)
- add structured commit footer API
- define canonical SASE content layout contract (sase-6d.1)

## [0.5.0](https://github.com/sase-org/sase-core/compare/v0.4.1...v0.5.0) - 2026-07-16

### Added

- *(editor)* add placeholder completion support (sase-6b.1)
- [**breaking**] require titles for all plans

## [0.4.0](https://github.com/sase-org/sase-core/compare/v0.3.4...v0.4.0) - 2026-07-14

### Added

- *(plan)* add strict frontmatter validation (sase-61.1)
- *(projects)* classify projects and canonicalize lifecycle (sase-5w.1)
- [**breaking**] remove legend-tier core support
- *(vcs)* add remote commit presence classification
- *(core)* add unified VCS commit-log parser and aggregator
- *(bead)* merge event streams for conflict resolution
- rename ChangeSpec wire review field

### Fixed

- *(prompt-stash)* bound shared lock acquisition

### Other

- Format with `cargo fmt`

## [0.3.2](https://github.com/sase-org/sase-core/compare/v0.3.1...v0.3.2) - 2026-07-06

### Added

- resolve agent family parents (sase-5f.3)

## [0.3.0](https://github.com/sase-org/sase-core/compare/v0.2.0...v0.3.0) - 2026-06-29

### Added

- add project display names to specs
- add prompt stash pin persistence

## [0.2.0](https://github.com/sase-org/sase-core/compare/v0.1.4...v0.2.0) - 2026-06-24

### Added

- *(config)* add Rust core config backend (sase-54.1)
- delete dismissed agent groups
- *(plan)* expose plan search via PyO3 binding (sase-4x.3)

## [0.1.3](https://github.com/sase-org/sase-core/compare/v0.1.2...v0.1.3) - 2026-06-18

### Added

- *(beads)* add core bead search CLI (sase-4w.2)
- *(editor)* add frontmatter schema & validation API (sase-4r.1)
- *(prompt-stash)* add prompt-stash store module and Python bindings (sase-4q.1)
- *(core)* [**breaking**] remove the episode module and PyO3 bindings
- add sharded agent artifact layout

### Fixed

- *(agent-index)* expose metadata and status helpers

### Other

- drain stale active artifact rows
- *(agent-scan)* query related artifact dirs from index

## [0.1.2](https://github.com/sase-org/sase-core/compare/v0.1.1...v0.1.2) - 2026-06-09

### Fixed

- expose agent template namespaces from core

## [0.1.1](https://github.com/sase-org/sase-core/releases/tag/v0.1.1) - 2026-06-08

### Added

- add agent name template primitives (sase-4g.1)
- add exact artifact dir scanner (sase-4f.2)
- add recent dismissed group archive APIs
- add ProjectSpec alias contract (sase-4c.1)
- add project lifecycle core contract (sase-49.1)
- add episode v2 wire contract (sase-48.1)
- add saved agent group archive backend (sase-47.1)
- add episode wire schema (sase-45.1)
- persist dismissed agent visibility in index (sase-3s.1)
- add visibility-aware index query and dismissed-agent sidecar (sase-3r.2)
- read bead stores from event logs (sase-3n.2)
- remove merged bead read exports (sase-3c.3)
- add Rust agent archive backend (sase-37.9)
- remove ChangeSpec test targets from core
- add counts-only notification append and rewrite APIs (sase-35.2)
- migrate Rust core to canonical .sase project spec extension (sase-33.4)
- add bead work preclaim mutation
- remove KICKSTART from core changespec wire
- remove unified artifact graph core
- add batched artifact summary contract
- add SQL-backed artifact search
- add paged artifact detail contract
- expose artifact graph export binding
- add artifact ingestion path framework
- expose artifact graph bindings (sase-23.1.6)
- add persistent agent artifact index
- Add bead tier metadata
- add Rust bead CLI planner
- add Rust bead mutation transactions
- add bead epic work planner
- add bead read bindings
- add Rust agent process spawn binding
- add Rust agent launch fanout planner
- add launch timestamp batch allocation
- add Rust launch preparation binding
- add Rust workspace claim planning
- add agent launch wire skeletons
- expose agent compose pyo3 binding
- add persistent query corpus PyO3 handles
- expose notification store PyO3 bindings
- add cleanup execution helpers
- add Rust agent cleanup planner
- Phase 5C — pure-Rust Git query parsers and PyO3 bindings (sase-1a.3)
- Phase 4C — pure-Rust status state machine and PyO3 bindings (sase-19.3)
- Phase 3C — sase_core_rs.scan_agent_artifacts PyO3 binding (sase-18.3)
- *(query)* Phase 2D PyO3 query bindings (sase-17.4)
- Phase 1D PyO3 binding exposing parse_project_bytes
- Phase 1A — Rust workspace and wire types (sase-16.1)

### Fixed

- add count-only notification state updates
- enforce artifact binding request parity

### Other

- add release-plz release automation (sase-4e.5)
- Revert "feat: add visibility-aware index query and dismissed-agent sidecar (sase-3r.2)"
- Revert "chore: restore Rust check formatting"
- restore Rust check formatting
- align rewrite_notifications counts binding test with merge semantics
- bump sase-core package version
- remove agent compose rust core
- Phase 6A — wheel packaging and release matrix (sase-1b.1)
