fn main() {
    if let Err(error) =
        sase_gateway::run_sudo_runner_cli(std::env::args().skip(1))
    {
        eprintln!("{}", error.message());
        std::process::exit(error.exit_code());
    }
}
