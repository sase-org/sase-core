//! Reviewed sudo runner.
//!
//! Two hosting modes are supported:
//!
//! - Native `sase_sudo_runner` binary: detached hops relaunch
//!   `std::env::current_exe()` with no extra prefix, so sudo runs
//!   `<current_exe> --internal-root-exec ...` and the root executor later
//!   spawns `<current_exe> --internal-root-worker ...`.
//! - PyO3 console script (`sase_core_rs.sudo_runner`): detached hops
//!   relaunch through the active interpreter as
//!   `<sys.executable> -I -m sase_core_rs.sudo_runner` plus the same
//!   internal mode arguments. Isolated mode (`-I`) is required so a
//!   lookalike `sase_core_rs` package cannot be imported from the reviewed
//!   working directory, `PYTHONPATH`, or the user site.
//!
//! The launcher program and prefix are host configuration, not
//! reviewed-manifest data and not public CLI options.
mod args;
mod config;
mod dispatch;
mod execution;
mod handoff;
mod platform;

#[cfg(all(test, unix))]
mod tests;

pub use config::{
    SudoRunnerCliError, SudoRunnerExitStatus, PYTHON_HOSTED_SUDO_RUNNER_PREFIX,
    SUDO_RUNNER_AUTH_FAILED_EXIT, SUDO_RUNNER_CANCELLED_EXIT,
    SUDO_RUNNER_INVALID_INPUT_EXIT, SUDO_RUNNER_RUNNER_ERROR_EXIT,
    SUDO_RUNNER_SUCCESS_EXIT, SUDO_RUNNER_TTY_UNAVAILABLE_EXIT,
};
pub use dispatch::{run_python_hosted_sudo_runner_cli, run_sudo_runner_cli};

pub(crate) use args::*;
pub(crate) use config::*;
pub(crate) use dispatch::*;
pub(crate) use execution::*;
pub(crate) use handoff::*;
pub(crate) use platform::*;
