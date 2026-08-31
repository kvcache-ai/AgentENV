use crate::client::{
    volumes::{CreateVolume, Volume, DEFAULT_VOLUME_SIZE_MB},
    Client,
};
use crate::output::{self, Format};
use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand, ValueEnum};
use tabled::Tabled;

#[derive(ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Sub,
}

#[derive(Subcommand)]
enum Sub {
    /// Create an empty, image-backed, or copy-on-write volume.
    Create {
        name: String,
        #[arg(
            long = "size-mb",
            default_value_t = DEFAULT_VOLUME_SIZE_MB,
            value_parser = parse_volume_size_mb
        )]
        size_mb: u64,
        #[arg(long, value_enum, default_value = "exclusive")]
        mode: Mode,
        #[arg(long = "from-volume", conflicts_with = "image")]
        from_volume: Option<String>,
        #[arg(long)]
        image: Option<String>,
    },
    /// List volumes.
    #[command(visible_alias = "ls")]
    List {
        #[arg(long, value_enum)]
        output: Option<Format>,
    },
    /// Inspect a volume by ID or name.
    Inspect { volume: String },
    /// Delete a volume by ID or name.
    Delete { volume: String },
}

fn parse_volume_size_mb(value: &str) -> std::result::Result<u64, String> {
    let size = value
        .parse::<u64>()
        .map_err(|_| "volume size must be a positive integer in MiB".to_string())?;
    (size > 0)
        .then_some(size)
        .ok_or_else(|| "volume size must be greater than zero".to_string())
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Ro,
    Exclusive,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ro => "ro",
            Self::Exclusive => "exclusive",
        }
    }
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    match args.cmd {
        Sub::Create {
            name,
            size_mb,
            mode,
            from_volume,
            image,
        } => {
            let volume = client.create_volume(&CreateVolume {
                name: &name,
                size_mb,
                mode: Some(mode.as_str()),
                from_volume: from_volume.as_deref(),
                image: image.as_deref(),
            })?;
            println!("Created volume {} ({})", volume.name, volume.volume_id);
            Ok(())
        }
        Sub::List { output } => list(&client, output::resolve(output)),
        Sub::Inspect { volume } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&client.get_volume(&volume)?)?
            );
            Ok(())
        }
        Sub::Delete { volume } => {
            client.delete_volume(&volume)?;
            println!("Deleted volume {volume}");
            Ok(())
        }
    }
}

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "VOLUME ID")]
    volume_id: String,
    name: String,
    mode: String,
    #[tabled(rename = "SIZE MB")]
    size_mb: String,
    status: String,
}

fn list(client: &Client, format: Format) -> Result<()> {
    let volumes = client.list_volumes()?;
    output::render(format, &volumes, |volume: &Volume| Row {
        volume_id: volume.volume_id.clone(),
        name: volume.name.clone(),
        mode: volume
            .mode
            .clone()
            .unwrap_or_else(|| "exclusive".to_owned()),
        size_mb: volume.size_mb.to_string(),
        status: volume.status.clone(),
    })
}
