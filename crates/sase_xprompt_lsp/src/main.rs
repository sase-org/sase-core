use std::io::Write;

use sase_xprompt_lsp::{logging, run_stdio};

#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        let mut stdout = std::io::stdout();
        let _ =
            writeln!(stdout, "sase-xprompt-lsp {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    logging::init();
    run_stdio().await;
}
