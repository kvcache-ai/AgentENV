use crate::client::{sandboxes::NewComposeSandbox, Client};
use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Subcommand};
use std::io::Read;
use std::path::PathBuf;

const MAX_COMPOSE_BYTES: u64 = 1024 * 1024;

#[derive(ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Create a sandbox, wait for Compose readiness, and print its ID
    Up(UpArgs),
}

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv compose up -f compose.yaml --cpu 2 --memory 2048
  aenv compose up -f compose.yaml --env TAG=stable --profile worker
  cat compose.yaml | aenv compose up -f -

Each invocation creates a new sandbox. Manage it with aenv exec, connect, pause,
resume, snapshot, and delete. Only explicit --env values are used for interpolation;
the local environment and .env files are not loaded.")]
struct UpArgs {
    /// Compose YAML or JSON file; use - to read stdin
    #[arg(short = 'f', long, default_value = "compose.yaml", value_name = "PATH")]
    file: PathBuf,
    /// Compose interpolation variable (repeatable; last value wins)
    #[arg(long = "env", value_name = "KEY=VALUE", value_parser = parse_env)]
    environment: Vec<(String, String)>,
    /// Enable an optional Compose profile (repeatable)
    #[arg(long = "profile", value_name = "NAME")]
    profiles: Vec<String>,
    /// Sandbox TTL in seconds, starting after Compose is ready
    #[arg(long, default_value_t = super::DEFAULT_TIMEOUT_SECS)]
    timeout: u32,
    /// Startup budget in seconds, including image resolution and health checks
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u32).range(1..=300))]
    startup_timeout: u32,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Root filesystem size in MiB (must be divisible by 1024)
    #[arg(long = "disk-size-mb", alias = "disk-mb", value_parser = super::parse_disk_size_mb)]
    disk_size_mb: Option<u32>,
}

pub(super) fn parse_env(value: &str) -> std::result::Result<(String, String), String> {
    let (key, value) = value
        .split_once('=')
        .filter(|(key, _)| !key.is_empty())
        .ok_or_else(|| "expected KEY=VALUE with a non-empty key".to_owned())?;
    Ok((key.to_owned(), value.to_owned()))
}

pub(super) fn read_compose(reader: impl Read) -> Result<String> {
    let mut compose = String::new();
    reader
        .take(MAX_COMPOSE_BYTES + 1)
        .read_to_string(&mut compose)
        .context("reading Compose source as UTF-8")?;
    anyhow::ensure!(
        compose.len() as u64 <= MAX_COMPOSE_BYTES,
        "Compose source exceeds 1 MiB"
    );
    anyhow::ensure!(!compose.trim().is_empty(), "Compose source is empty");
    Ok(compose)
}

pub fn run(args: Args) -> Result<()> {
    match args.cmd {
        Sub::Up(args) => up(args),
    }
}

fn up(args: UpArgs) -> Result<()> {
    let compose = if args.file == std::path::Path::new("-") {
        read_compose(std::io::stdin().lock())?
    } else {
        let file = std::fs::File::open(&args.file)
            .with_context(|| format!("opening Compose file {}", args.file.display()))?;
        read_compose(file)?
    };
    let body = NewComposeSandbox {
        compose: &compose,
        compose_env: args.environment.into_iter().collect(),
        profiles: args.profiles,
        timeout: args.timeout,
        startup_timeout: args.startup_timeout,
        cpu_count: args.resources.cpu_count,
        memory_mb: args.resources.memory_mb,
        disk_size_mb: args.disk_size_mb,
    };
    let client = Client::from_env()?;
    eprintln!("Starting Compose sandbox; waiting for services to become ready...");
    let sandbox = client.create_compose_sandbox(&body)?;
    println!("{}", sandbox.sandbox_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    fn parse(arguments: &[&str]) -> UpArgs {
        let crate::Cmd::Compose(Args { cmd: Sub::Up(args) }) =
            crate::Cli::try_parse_from(arguments).unwrap().cmd
        else {
            panic!("expected compose up");
        };
        args
    }

    #[test]
    fn accepts_profiles_explicit_environment_and_resource_aliases() {
        crate::Cli::command().debug_assert();
        let args = parse(&[
            "aenv",
            "compose",
            "up",
            "-f",
            "-",
            "--profile",
            "web",
            "--profile",
            "worker",
            "--env",
            "VALUE=a=b",
            "--env",
            "EMPTY=",
            "--cpu-count",
            "2",
            "--mem",
            "2048",
            "--disk-mb",
            "8192",
            "--timeout",
            "600",
            "--startup-timeout",
            "60",
        ]);
        assert_eq!(args.file, PathBuf::from("-"));
        assert_eq!(args.profiles, ["web", "worker"]);
        assert_eq!(
            args.environment,
            [("VALUE".into(), "a=b".into()), ("EMPTY".into(), "".into())]
        );
        assert_eq!(args.resources.cpu_count, Some(2));
        assert_eq!(args.resources.memory_mb, Some(2048));
        assert_eq!(args.disk_size_mb, Some(8192));
        assert_eq!((args.timeout, args.startup_timeout), (600, 60));
        let defaults = parse(&["aenv", "compose", "up"]);
        assert_eq!(defaults.file, PathBuf::from("compose.yaml"));
        assert!(defaults.environment.is_empty());
        assert!(defaults.profiles.is_empty());
        assert!(!defaults.resources.is_set());
        assert_eq!((defaults.timeout, defaults.startup_timeout), (300, 300));
    }

    #[test]
    fn rejects_invalid_environment_disk_and_startup_budgets() {
        for (flag, value) in [
            ("--env", "MISSING_VALUE"),
            ("--env", "=value"),
            ("--disk-size-mb", "0"),
            ("--disk-size-mb", "1025"),
            ("--startup-timeout", "0"),
            ("--startup-timeout", "301"),
        ] {
            assert!(crate::Cli::try_parse_from(["aenv", "compose", "up", flag, value]).is_err());
        }
    }

    #[test]
    fn preserves_compose_source_and_bounds_input() {
        let source = "services: {app: {image: '${IMAGE}', command: ['echo', '$$HOME']}}\n";
        assert_eq!(read_compose(source.as_bytes()).unwrap(), source);
        assert!(read_compose(" \n".as_bytes()).is_err());
        assert!(read_compose(&[0xff][..]).is_err());
        let maximum = vec![b'x'; MAX_COMPOSE_BYTES as usize];
        assert!(read_compose(maximum.as_slice()).is_ok());
        assert!(read_compose(std::io::repeat(b'x')).is_err());
    }
}
