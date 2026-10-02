use std::io::Write;

use sase_xprompt_lsp::{logging, run_stdio};

#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        let mut stdout = std::io::stdout();
        let binary_name = std::env::current_exe()
            .ok()
            .and_then(|path| {
                path.file_name()?.to_string_lossy().into_owned().into()
            })
            .map(|name: String| {
                if name.starts_with("sase-macro-lsp") {
                    "sase-macro-lsp".to_string()
                } else {
                    "sase-xprompt-lsp".to_string()
                }
            })
            .unwrap_or_else(|| "sase-xprompt-lsp".to_string());
        let _ = writeln!(stdout, "{binary_name} {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    logging::init();
    run_stdio().await;
}
