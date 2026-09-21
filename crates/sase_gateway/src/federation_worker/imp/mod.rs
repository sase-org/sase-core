//! Unix implementation of the federation worker.
//!
//! The real worker only runs on Unix (local-socket IPC with peer-credential
//! checks). This module holds that implementation, split by responsibility:
//!
//! - [`listener`] runs the accept loop and owns socket lifecycle plus
//!   accept-time admission (directory permissions, symlink rejection,
//!   peer-UID check, connection permits).
//! - [`framing`] covers length-prefixed frame I/O, request dispatch, and
//!   deadline enforcement.
//! - [`state`] owns [`FederationWorkerState`](state::FederationWorkerState),
//!   the shared worker state behind every operation.
//! - [`remote_hosts`] talks to remote fleet hosts (validation, TLS client,
//!   per-host reads with cache fallback).
//! - [`cache`] persists the on-disk response cache.

use super::*;

mod cache;
mod framing;
mod listener;
mod remote_hosts;
mod state;

#[cfg(test)]
mod tests;

pub use listener::run;

use cache::*;
use framing::*;
use remote_hosts::*;
use state::*;
