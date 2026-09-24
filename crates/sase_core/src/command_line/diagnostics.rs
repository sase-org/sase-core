use crate::command_line::wire::LineDiagnosticWire;

#[derive(Debug, Clone)]
pub struct RawDiagnostic {
    pub start: usize,
    pub end: usize,
    pub severity: String,
    pub code: String,
    pub message: String,
    pub on_cursor_token: bool,
}

impl RawDiagnostic {
    pub fn wire(self) -> LineDiagnosticWire {
        LineDiagnosticWire {
            start: self.start,
            end: self.end,
            severity: self.severity,
            code: self.code,
            message: self.message,
        }
    }
}

pub fn sort_diagnostics(diags: &mut [RawDiagnostic]) {
    diags.sort_by(|a, b| {
        a.start.cmp(&b.start).then_with(|| a.code.cmp(&b.code))
    });
}
