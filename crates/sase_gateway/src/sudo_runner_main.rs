//! Native `sase_sudo_runner` binary.
//!
//! Detached hops relaunch this executable with `--internal-root-exec` and
//! `--internal-root-worker` and no extra prefix. The PyO3 console script
//! uses `run_python_hosted_sudo_runner_cli` instead.

fn main() {
    if let Err(error) =
        sase_gateway::run_sudo_runner_cli(std::env::args().skip(1))
    {
        eprintln!("{}", error.message());
        std::process::exit(error.exit_code());
    }
}
