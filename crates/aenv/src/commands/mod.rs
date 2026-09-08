pub mod auth;
pub mod build;
pub mod completion;
pub mod connect;
pub mod cpu_bind;
pub mod delete;
pub mod download;
pub mod exec;
pub mod list;
pub mod pause;
pub mod pull;
pub mod resume;
pub mod snapshot;
pub mod start;
pub mod template;
pub mod timeout;
pub mod upload;
pub mod volume;

use crate::client::Client;
use anyhow::Result;
use clap::Args as ClapArgs;

pub const DEFAULT_TIMEOUT_SECS: u32 = 300;

#[derive(Clone, Copy, Debug, Default, ClapArgs)]
pub struct CpuMemoryArgs {
    /// CPU cores to request
    #[arg(long = "cpu", alias = "cpu-count", value_name = "COUNT")]
    pub cpu_count: Option<u32>,
    /// Memory in MiB to request
    #[arg(
        long = "memory",
        alias = "memory-mb",
        alias = "mem",
        value_name = "MIB"
    )]
    pub memory_mb: Option<u32>,
}

impl CpuMemoryArgs {
    pub fn is_set(&self) -> bool {
        self.cpu_count.is_some() || self.memory_mb.is_some()
    }
}

/// Accepts either a UUID template ID or a template name/alias and returns the UUID.
pub fn resolve_template(client: &Client, arg: &str) -> Result<String> {
    if uuid::Uuid::parse_str(arg).is_ok() {
        return Ok(arg.to_string());
    }
    client.resolve_alias(arg)
}

pub fn tokio_rt() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}
