mod compile;
mod wire;

#[cfg(test)]
mod tests;

pub use compile::{compile_agent_archive_corpus, AgentArchiveCompileError};
pub use wire::{
    AgentArchiveCompileRequestWire, AgentArchiveContainerWire,
    AgentArchiveCorpusRowWire, AgentArchiveCorpusStatusWire,
    AgentArchiveCorpusWire, AgentArchiveIndexProbeStatusWire,
    AgentArchiveLinkFacetsWire, AgentArchiveNameMatchWire,
    AgentArchiveOutcomeWire, AgentArchiveTimeBasisWire,
    AgentArchiveUnsupportedIndexWire,
};
