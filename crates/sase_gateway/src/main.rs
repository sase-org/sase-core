fn main() {
    if let Err(err) = sase_gateway::run_gateway_cli(std::env::args().skip(1)) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
