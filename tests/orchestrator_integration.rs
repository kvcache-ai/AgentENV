mod common;

// Orchestrator shutdown permanently stops process-global runtime managers, so
// run all scenarios in one test runtime and shut it down only after the last VM.
#[path = "integration/orchestrator.rs"]
mod orchestrator;
