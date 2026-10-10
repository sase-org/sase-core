mod compile;
mod query;
mod wire;

#[cfg(test)]
mod tests;

pub use compile::{compile_agent_archive_corpus, AgentArchiveCompileError};
pub use query::{
    count_agent_archive_corpus, lookup_agent_archive_corpus,
    rows_agent_archive_corpus, summarize_agent_archive_corpus,
};
pub use wire::{
    AgentArchiveCompileRequestWire, AgentArchiveContainerWire,
    AgentArchiveCorpusRowWire, AgentArchiveCorpusStatusWire,
    AgentArchiveCorpusWire, AgentArchiveCountRequestWire,
    AgentArchiveIndexProbeStatusWire, AgentArchiveLightRowWire,
    AgentArchiveLinkFacetsWire, AgentArchiveLookupRequestWire,
    AgentArchiveNameMatchWire, AgentArchiveOutcomeWire,
    AgentArchiveQueryCountWire, AgentArchiveQueryError,
    AgentArchiveQueryGroupWire, AgentArchiveQueryLookupWire,
    AgentArchiveQueryRowsWire, AgentArchiveQuerySummaryWire,
    AgentArchiveRowsRequestWire, AgentArchiveSummaryRequestWire,
    AgentArchiveTimeBasisWire, AgentArchiveUnsupportedIndexWire,
};
