# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3](https://github.com/sase-org/sase-core/compare/v0.1.2...v0.1.3) - 2026-06-13

### Added

- add sharded agent artifact layout

### Fixed

- *(agent-index)* expose metadata and status helpers
- quarantine corrupt artifact index sidecars

### Other

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
- allow legend beads to be marked ready
- add legend work planner
- add legend epic count bead metadata
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
- resolve legend launch tag in epic work plans
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
