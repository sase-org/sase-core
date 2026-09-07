fn main() {
    if let Err(error) =
        sase_gateway::run_federation_worker_cli(std::env::args().skip(1))
    {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
